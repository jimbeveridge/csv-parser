/** @file
 *  @brief Defines functionality needed for basic CSV parsing
 */

#include "csv_reader.hpp"

namespace csv {
    namespace internals {
        /** Return a CSV's column names
         *
         *  @param[in] filename  Path to CSV file
         *  @param[in] format    Format of the CSV file
         *
         */
        CSV_INLINE std::vector<std::string> _get_col_names(csv::string_view head, CSVFormat format) {
            // Parse the CSV
            auto trim_chars = format.get_trim_chars();

            StringViewParser parser(head, format);
            parser.set_max_rows(format.get_header()+1);
            parser.read_csv();

            CSVRow row;
            while (parser._channel.try_dequeue(row)) {}
            return std::move(row);
        }

        CSV_INLINE GuessScore calculate_score(csv::string_view head, CSVFormat format) {
            // Frequency counter of row length
            std::unordered_map<size_t, size_t> row_tally = { { 0, 0 } };

            // Map row lengths to row num where they first occurred
            std::unordered_map<size_t, size_t> row_when = { { 0, 0 } };

            // Parse the CSV. We are parsing in the current thread, so once next()
            // completes, the data in _channel will not update asynchronously.
            StringViewParser parser(head, format);
            parser.set_max_rows(30);
            parser.read_csv();

            CSVRow row;
            for (int i = 0; parser._channel.try_dequeue(row); ++i) {
                // Ignore zero-length rows
                if (row.size() > 0) {
                    if (row_tally.find(row.size()) != row_tally.end()) {
                        row_tally[row.size()]++;
                    }
                    else {
                        row_tally[row.size()] = 1;
                        row_when[row.size()] = i;
                    }
                }
            }

            double final_score = 0.0;
            size_t header_row = 0;

            // Final score is equal to the largest
            // row size times rows of that size
            for (auto& pair : row_tally) {
                auto row_size = pair.first;
                auto row_count = pair.second;
                double score = (double)(row_size * row_count);
                if (score > final_score) {
                    final_score = score;
                    header_row = row_when[row_size];
                }
            }

            return {
                final_score,
                header_row
            };
        }

        /** Guess the delimiter used by a delimiter-separated values file */
        CSV_INLINE CSVGuessResult _guess_format(csv::string_view head, const std::vector<char>& delims) {
            /** For each delimiter, find out which row length was most common.
             *  The delimiter with the longest mode row length wins.
             *  Then, the line number of the header row is the first row with
             *  the mode row length.
             */

            CSVFormat format;
            size_t max_score = 0,
                header = 0;
            char current_delim = delims[0];

            for (char cand_delim : delims) {
                auto result = calculate_score(head, format.delimiter(cand_delim));

                if ((size_t)result.score > max_score) {
                    max_score = (size_t)result.score;
                    current_delim = cand_delim;
                    header = result.header;
                }
            }

            return { current_delim, (int)header };
        }
    }

    /** Return a CSV's column names
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     */
    CSV_INLINE std::vector<std::string> get_col_names(csv::string_view filename, CSVFormat format) {
        auto head = internals::get_csv_head(filename);

        /** Guess delimiter and header row */
        if (format.guess_delim()) {
            auto guess_result = guess_format(filename, format.get_possible_delims());
            format.delimiter(guess_result.delim).header_row(guess_result.header_row);
        }

        return internals::_get_col_names(head, format);
    }

    /** Guess the delimiter used by a delimiter-separated values file */
    CSV_INLINE CSVGuessResult guess_format(csv::string_view filename, const std::vector<char>& delims) {
        auto head = internals::get_csv_head(filename);
        return internals::_guess_format(head, delims);
    }

    /** Reads an arbitrarily large CSV file using memory-mapped IO.
     *
     *  **Details:** Reads the first block of a CSV file synchronously to get information
     *               such as column names and delimiting character.
     *
     *  @param[in] filename  Path to CSV file
     *  @param[in] format    Format of the CSV file
     *
     *  \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
	CSV_INLINE CSVReader::CSVReader(csv::string_view filename, CSVFormat format) {
        /** Guess delimiter and header row */
        if (format.guess_delim()) {
            auto head = internals::get_csv_head(filename);
            auto guess_result = internals::_guess_format(head, format.possible_delimiters);
            format.delimiter(guess_result.delim);
            format.header = guess_result.header_row;
        }

        this->parser = std::unique_ptr<internals::MmapParser>(
            new internals::MmapParser(filename, format, this->col_names)); // For C++11

        this->_format = format;

        if (!format.col_names.empty())
            this->set_col_names(format.col_names);

        launch_worker_thread();
    }

    /** Return the format of the original raw CSV */
    CSV_INLINE CSVFormat CSVReader::get_format() const {
        CSVFormat new_format = this->_format;

        // Since users are normally not allowed to set
        // column names and header row simultaneously,
        // we will set the backing variables directly here
        new_format.col_names = this->col_names->get_col_names();
        new_format.header = this->_format.header;

        return new_format;
    }

    /** Return the CSV's column names as a vector of strings. */
    CSV_INLINE const std::vector<std::string>& CSVReader::get_col_names() const {
        if (this->col_names) {
            return this->col_names->get_col_names();
        }

        static const std::vector<std::string> empty;
        return empty;
    }

    /** Return the index of the column name if found or
     *         csv::CSV_NOT_FOUND otherwise.
     */
    CSV_INLINE int CSVReader::index_of(csv::string_view col_name) const {
        auto _col_names = this->get_col_names();
        for (size_t i = 0; i < _col_names.size(); i++)
            if (_col_names[i] == col_name) return (int)i;

        return CSV_NOT_FOUND;
    }

    CSV_INLINE bool CSVReader::trim_header() {
        if (this->header_trimmed)
        {
            return true;
        }

        auto policy = this->_format.variable_column_policy;
        this->_format.variable_column_policy = VariableColumnPolicy::KEEP;

        bool success = true;
        CSVRow row;

        // It's legal for the caller to set the header count and also
        // provide their own col names. In this case we still need to
        // strip the header row, but don't do anything with the data.
        for (int i = 0; i <= this->_format.header; i++) {
            success = this->read_row(row);
            if (!success)
                break;
            if (i == this->_format.header && this->col_names->empty()) {
                this->set_col_names(row);
            }
        }

        // The header lines do not count as data rows.
        this->_n_data_rows = 0;

        this->header_trimmed = true;
        this->_format.variable_column_policy = policy;
        return success;
    }

    /**
     *  @param[in] names Column names
     */
    CSV_INLINE void CSVReader::set_col_names(const std::vector<std::string>& names)
    {
        this->col_names->set_col_names(names);
        this->n_cols = names.size();
    }

    // Will throw an exception if there's an issue with the file, such as it doesn't exist.
    CSV_INLINE void CSVReader::launch_worker_thread()
    {
        this->read_csv_worker = std::thread([this] { this->parser->read_csv(internals::ITERATION_CHUNK_SIZE); });

        // TODO - Move this out of the startup path. Maybe to read_row? Careful of recursive call.
        if (!this->trim_header())
        {
            stop_thread_and_join();
            return;
        }
    }

    CSV_INLINE void CSVReader::stop_thread_and_join()
    {
        this->parser->stop_thread_and_join(this->read_csv_worker);
    }
}
