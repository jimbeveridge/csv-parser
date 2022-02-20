#include "basic_csv_parser.hpp"

namespace csv {

    /**
     * Retrieve rows as CSVRow objects. Returns false if no more rows are available.
     *
     * @param[out] row The variable where the parsed row will be stored
     * @see CSVRow, CSVField
     *
     * **Example:**
     * \snippet tests/test_read_csv.cpp CSVField Example
     *
     */
    CSV_INLINE bool CSVQueueReader::read_row(CSVRow& row) {

        if (_received_tombstone) {
            return false;
        }

        channel().wait_dequeue(row);
        if (this->teptr)
        {
            std::rethrow_exception(teptr);
        }
        if (row.tombstone())
        {
            _received_tombstone = true;
            return false;
        }

        ++_n_rows;

        return true;
    }

    namespace internals {
        CSV_INLINE std::string format_row(const std::vector<std::string>& row, csv::string_view delim) {
            /** Print a CSV row */
            std::stringstream ret;
            for (size_t i = 0; i < row.size(); i++) {
                ret << row[i];
                if (i + 1 < row.size()) ret << delim;
                else ret << '\n';
            }
            ret.flush();

            return ret.str();
        }

        CSV_INLINE size_t get_file_size(csv::string_view filename) {
            std::ifstream infile(std::string(filename), std::ios::binary);
            if (!infile) {
                throw std::runtime_error("Cannot open file " + std::string(filename));
            }

            const auto start = infile.tellg();
            infile.seekg(0, std::ios::end);
            const auto end = infile.tellg();

            return end - start;
        }

        CSV_INLINE std::string get_csv_head(csv::string_view filename) {
            return get_csv_head(filename, get_file_size(filename));
        }

        CSV_INLINE std::string get_csv_head(csv::string_view filename, size_t file_size) {
            const size_t bytes = 500000;

            std::error_code error;
            size_t length = std::min((size_t)file_size, bytes);
            if (length == 0) {
                return "";
            }

            auto mmap = mio::make_mmap_source(std::string(filename), 0, length, error);

            if (error) {
                throw std::runtime_error("Cannot open file " + std::string(filename));
            }

            return std::string(mmap.begin(), mmap.end());
        }

#ifdef _MSC_VER
#pragma region IBasicCVParser
#endif
        CSV_INLINE IBasicCSVParser::IBasicCSVParser(
            const CSVFormat& format,
            const ColNamesPtr& col_names
        ) : _col_names(col_names) {

            if (format.no_quote) {
                _parse_flags = internals::make_parse_flags(format.get_delim());
            }
            else {
                _parse_flags = internals::make_parse_flags(format.get_delim(), format.quote_char);
            }

            _ws_flags = internals::make_ws_flags(
                format.trim_chars.data(), format.trim_chars.size()
            );
        }

        /**
         * Read all of the CSV data, unless interrupted.
         *
         * @note This method usually runs on its own thread.
         *
         * @param[in] bytes Number of bytes to read.
         *
         * @see CSVReader::read_row()
         */
        CSV_INLINE bool IBasicCSVParser::read_csv(size_t bytes) {
            set_iteration_chunk_size(bytes);
            bool result = true;
            try
            {
                while (!eof() && !_terminateNow)
                {
                    this->next();
                }
            }
            catch (...)
            {
                this->queue_reader.set_current_exception();
                result = false;
            }

            this->enqueue_tombstone();

            return result;
        }

        CSV_INLINE void IBasicCSVParser::stop_thread_and_join(std::thread& read_csv_worker)
        {
            this->_terminateNow = true;

            CSVRow row;
            while (!this->queue_reader.eof() && !row.tombstone() && read_csv_worker.joinable())
            {
                // There could be many rows pending in the queue.
                this->_channel.wait_dequeue_timed(row, 10000);
            }

            if (read_csv_worker.joinable()) {
                // If the circular buffer is full, make space for the other thread to loop.
                this->_channel.try_dequeue(row);
                this->_channel.try_dequeue(row);

                read_csv_worker.join();
            }
        }

        CSV_INLINE void IBasicCSVParser::end_feed() {
            using internals::ParseFlags;

            bool empty_last_field = this->data_ptr
                && (this->data_ptr->_dataString || data_ptr->_dataMmap)
                && !this->data_ptr->data.empty()
                && parse_flag(this->data_ptr->data.back()) == ParseFlags::DELIMITER;

            // Push field
            if (this->field_length > 0 || empty_last_field) {
                this->push_field();
            }

            // Push row
            if (this->current_row.size() > 0)
                this->push_row();
        }

        CSV_INLINE void IBasicCSVParser::enqueue_tombstone()
        {
            this->current_row = CSVRow(&_pa, nullptr);
            //this->current_row = CSVRow({}, this->data_pos, this->current_row.fields.size());
            _channel.wait_enqueue(std::move(current_row));
        }

        CSV_INLINE void IBasicCSVParser::parse_field() noexcept {
            using internals::ParseFlags;
            auto& in = this->data_ptr->data;

            // Trim off leading whitespace
            while (data_pos < in.size() && ws_flag(in[data_pos]))
                data_pos++;

            if (field_start == UNINITIALIZED_FIELD)
                field_start = (int)(data_pos - current_row_start());

            // Optimization: Since NOT_SPECIAL characters tend to occur in contiguous
            // sequences, use the loop below to avoid having to go through the outer
            // switch statement as much as possible
            while (data_pos < in.size() && compound_parse_flag(in[data_pos]) == ParseFlags::NOT_SPECIAL)
                data_pos++;

            field_length = data_pos - (field_start + current_row_start());

            // Trim off trailing whitespace, this->field_length constraint matters
            // when field is entirely whitespace
            for (size_t j = data_pos - 1; ws_flag(in[j]) && this->field_length > 0; j--)
                this->field_length--;
        }

        CSV_INLINE void IBasicCSVParser::push_field()
        {
            current_row.fields.emplace_back(
                field_start == UNINITIALIZED_FIELD ? 0 : (unsigned int)field_start,
                field_length,
                field_has_double_quote
            );

            // Reset field state
            field_has_double_quote = false;
            field_length = 0;
            field_start = UNINITIALIZED_FIELD;

            current_row.row_length++;
        }

        CSV_INLINE void IBasicCSVParser::processNewline()
        {
            // End of record -> Write record
            this->push_field();
            auto size = std::max(!this->_col_names ? 0 : this->_col_names->size(), this->current_row.fields.size());
            this->push_row();

            // Reset
            this->current_row = CSVRow(&this->_pa, data_ptr, this->data_pos, current_row.fields.size());
            this->current_row.fields.reserve(size);
        }

        /** @return The number of characters parsed that belong to complete rows */
        CSV_INLINE size_t IBasicCSVParser::parse(bool forceNewline)
        {
            using internals::ParseFlags;

            this->quote_escape = false;
            this->data_pos = 0;
            this->current_row_start() = 0;

            this->trim_utf8_bom();

            auto& in = this->data_ptr->data;
            while (this->data_pos < in.size()) {
                switch (compound_parse_flag(in[this->data_pos])) {
                case ParseFlags::DELIMITER:
                    this->push_field();
                    this->data_pos++;
                    break;

                case ParseFlags::NEWLINE:
                    if (this->_terminateNow)
                    {
                        this->_eof = true;
                        return 0;
                    }

                    this->data_pos++;
                    // Skip past a two-character CRLF (or LFLF)
                    // There's a corner case if the CRLF/LFLF is split across two chunks.
                    // That's handled elsewhere by ignoring a newline at the beginning of
                    // a chunk after the first chunk.
                    if (this->data_pos < in.size() && parse_flag(in[this->data_pos]) == ParseFlags::NEWLINE)
                        this->data_pos++;

                    processNewline();
                    if (_max_rows == 0)
                    {
                        this->_eof = true;
                        return 0;
                    }
                    break;

                case ParseFlags::NOT_SPECIAL:
                    this->parse_field();
                    break;

                case ParseFlags::QUOTE_ESCAPE_QUOTE:
                    if (data_pos + 1 == in.size()) return this->current_row_start();
                    else if (data_pos + 1 < in.size()) {
                        auto next_ch = parse_flag(in[data_pos + 1]);
                        if (next_ch >= ParseFlags::DELIMITER) {
                            quote_escape = false;
                            data_pos++;
                            break;
                        }
                        else if (next_ch == ParseFlags::QUOTE) {
                            // Case: Escaped quote
                            data_pos += 2;
                            this->field_length += 2;
                            this->field_has_double_quote = true;
                            break;
                        }
                    }
                    
                    // Case: Unescaped single quote => not strictly valid but we'll keep it
                    this->field_length++;
                    data_pos++;

                    break;

                default: // Quote (currently not quote escaped)
                    if (this->field_length == 0) {
                        quote_escape = true;
                        data_pos++;
                        if (field_start == UNINITIALIZED_FIELD && data_pos < in.size() && !ws_flag(in[data_pos]))
                            field_start = (int)(data_pos - current_row_start());
                        break;
                    }

                    // Case: Unescaped quote
                    this->field_length++;
                    data_pos++;

                    break;
                }
            }

            if (forceNewline && this->data_pos > 0 && compound_parse_flag(in[this->data_pos-1]) != ParseFlags::NEWLINE) {
                processNewline();
            }

            return this->current_row_start();
        }

        CSV_INLINE void IBasicCSVParser::push_row() {
            current_row.row_length = current_row.fields.size() - current_row.fields_start;
            _channel.wait_enqueue(std::move(current_row));
            if (_max_rows > 0) {
                --_max_rows;
            }
        }

        CSV_INLINE void IBasicCSVParser::reset_data_ptr() {
            this->data_ptr = std::make_shared<RawCSVData>();
            this->data_ptr->parse_flags = this->_parse_flags;
            this->data_ptr->col_names = this->_col_names;
            //this->current_row.fields = &(this->data_ptr->fields);
        }

        CSV_INLINE void IBasicCSVParser::trim_utf8_bom() {
            auto& data = this->data_ptr->data;

            if (!this->unicode_bom_scan && data.size() >= 3) {
                if (data[0] == '\xEF' && data[1] == '\xBB' && data[2] == '\xBF') {
                    this->data_pos += 3; // Remove BOM from input string
                    this->_bomPromise.set_value(true);
                }
                else {
                    this->_bomPromise.set_value(false);
                }

                this->unicode_bom_scan = true;
            }
        }

#ifdef _MSC_VER
#pragma endregion
#endif

#ifdef _MSC_VER
#pragma region Specializations
#endif
        CSV_INLINE void StringViewParser::next() {
            if (this->eof()) return;

            this->reset_data_ptr();

            if (source_size == 0) {
                source_size = _source.size();
            }

            // Read data into buffer
            size_t length = std::min(source_size - stream_pos, this->_iteration_chunk_size);
            stream_pos = this->data_ptr->CreateStringViewSource(_source, stream_pos, length);

            // Parse
            this->current_row = CSVRow(&_pa, this->data_ptr);
            bool forceNewline = (stream_pos + this->_iteration_chunk_size >= source_size);
            size_t completed = this->parse(forceNewline);
            this->stream_pos -= (length - completed);

            if (eof() || stream_pos == source_size || no_chunk()) {
                this->_eof = true;
                this->end_feed();
            }
        }

        CSV_INLINE void MmapParser::next() {
            // Reset parser state
            this->field_start = UNINITIALIZED_FIELD;
            this->field_length = 0;
            this->reset_data_ptr();

            size_t length = std::min(this->source_size - this->mmap_pos, this->_iteration_chunk_size);
            const bool forceNewline = length < this->_iteration_chunk_size;
            if (length > 0) {
                // Create memory map
                this->mmap_pos = this->data_ptr->CreateMmapSource(this->_filename, this->mmap_pos, length);

                // Parse
                this->current_row = CSVRow(&_pa, this->data_ptr);
                // If we are at the end of the file, parse the last line even if there's no newline
                size_t completed = this->parse(forceNewline);
                this->mmap_pos -= (length - completed);
            }

            if (this->mmap_pos == this->source_size || no_chunk()) {
                this->_eof = true;
                this->end_feed();
            }
        }
#ifdef _MSC_VER
#pragma endregion
#endif
    }
}
