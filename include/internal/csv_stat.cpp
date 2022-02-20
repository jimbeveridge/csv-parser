/** @file
 *  Calculates statistics from CSV files
 */

#include <string>
#include <execution>
#include <string>
#include <unordered_map>
#include <vector>
#include "csv_stat.hpp"

namespace csv {

    constexpr size_t CALC_CHUNK_SIZE = 5000;

    /** Calculate statistics for an arbitrarily large file. When this constructor
     *  is called, CSVStat will process the entire file iteratively. Once finished,
     *  methods like get_mean(), get_counts(), etc... can be used to retrieve statistics.
     */
    CSV_INLINE CSVStat::CSVStat(csv::string_view filename, CSVFormat format) :
        reader(filename, format) {
        this->calc();
    }

    /** Calculate statistics for a CSV stored in a std::stringstream */
    CSV_INLINE CSVStat::CSVStat(std::stringstream& stream, CSVFormat format) :
        reader(stream, format) {
        this->calc();
    }

    /** Return current means */
    CSV_INLINE std::vector<long double> CSVStat::get_mean() const {
        return this->rolling_means;
    }

    /** Return current variances */
    CSV_INLINE std::vector<long double> CSVStat::get_variance() const {
        std::vector<long double> ret;        
        ret.reserve(this->get_col_names().size());
        for (size_t i = 0; i < this->get_col_names().size(); i++) {
            ret.push_back(this->rolling_vars[i]/(this->n[i] - 1));
        }
        return ret;
    }

    /** Return current mins */
    CSV_INLINE std::vector<long double> CSVStat::get_mins() const {
        return this->mins;
    }

    /** Return current maxes */
    CSV_INLINE std::vector<long double> CSVStat::get_maxes() const {
        return this->maxes;
    }

    /** Get counts for each column */
    CSV_INLINE std::vector<CSVStat::FreqCount> CSVStat::get_counts() const {
        return this->counts;
    }

    /** Get data type counts for each column */
    CSV_INLINE std::vector<CSVStat::TypeCount> CSVStat::get_dtypes() const {
        return this->dtypes;
    }

    CSV_INLINE void CSVStat::calc_chunk() {
        const size_t count = this->get_col_names().size();

        // Threads take time to start. Don't be too eager to do so.
        CountedRange counter(count);
        if (count > 100) {
            std::for_each(std::execution::par_unseq, counter.begin(), counter.end(),
                [this](size_t i) { calc_worker(i); });
        }
        else {
            std::for_each(std::execution::seq, counter.begin(), counter.end(),
                [this](size_t i) { calc_worker(i); });
        }

        this->records.clear();
    }

    CSV_INLINE void CSVStat::calc() {
        const size_t count = this->get_col_names().size();

        dtypes.resize(count);
        counts.resize(count);
        rolling_means.resize(count);
        rolling_vars.resize(count);
        mins.resize(count, NAN);
        maxes.resize(count, NAN);
        n.resize(count);

        for (const auto& row : reader) {
            this->records.push_back(std::move(row));

            /** Chunk rows */
            if (this->records.size() == CALC_CHUNK_SIZE) {
                calc_chunk();
            }
        }

        if (!this->records.empty()) {
          calc_chunk();
        }
    }

    CSV_INLINE void CSVStat::calc_worker(const size_t &i) {
        /** Worker thread for CSVStat::calc() which calculates statistics for one column.
         * 
         *  @param[in] i Column index
         */

        auto current_record = this->records.begin();

        for (size_t processed = 0; current_record != this->records.end(); processed++) {
            if (current_record->size() == this->get_col_names().size()) {
                auto current_field = (*current_record)[i];

                // Optimization: Don't count() if there's too many distinct values in the first 1000 rows
                if (processed < 1000 || this->counts[i].size() <= 500)
                    this->count(current_field, i);

                this->dtype(current_field, i);

                // Numeric Stuff
                if (current_field.is_num()) {
                    long double x_n = current_field.get<long double>();

                    // This actually calculates mean AND variance
                    this->variance(x_n, i);
                    this->min_max(x_n, i);
                }
            }
            else if (this->reader.get_format().get_variable_column_policy() == VariableColumnPolicy::THROW) {
                throw std::runtime_error("Line has different length than the others " + internals::format_row(*current_record));
            }

            ++current_record;
        }
    }

    CSV_INLINE void CSVStat::dtype(CSVField& data, const size_t &i) {
        /** Given a record update the type counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */
        
        auto type = data.type();
        this->dtypes[i][type]++;
    }

    CSV_INLINE void CSVStat::count(CSVField& data, const size_t &i) {
        /** Given a record update the frequency counter
         *  @param[in]  record Data observation
         *  @param[out] i      The column index that should be updated
         */

        auto item = data.get<std::string>();
        this->counts[i][item]++;
    }

    CSV_INLINE void CSVStat::min_max(const long double &x_n, const size_t &i) {
        /** Update current minimum and maximum
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
        if (std::isnan(this->mins[i]))
            this->mins[i] = x_n;
        if (std::isnan(this->maxes[i]))
            this->maxes[i] = x_n;
        
        if (x_n < this->mins[i])
            this->mins[i] = x_n;
        else if (x_n > this->maxes[i])
            this->maxes[i] = x_n;
    }

    CSV_INLINE void CSVStat::variance(const long double &x_n, const size_t &i) {
        /** Given a record update rolling mean and variance for all columns
         *  using Welford's Algorithm
         *  @param[in]  x_n Data observation
         *  @param[out] i   The column index that should be updated
         */
        long double& current_rolling_mean = this->rolling_means[i];
        long double& current_rolling_var = this->rolling_vars[i];
        long double& current_n = this->n[i];
        long double delta;
        long double delta2;

        current_n++;
        
        if (current_n == 1) {
            current_rolling_mean = x_n;
        } else {
            delta = x_n - current_rolling_mean;
            current_rolling_mean += delta/current_n;
            delta2 = x_n - current_rolling_mean;
            current_rolling_var += delta*delta2;
        }
    }

    /** Useful for uploading CSV files to SQL databases.
     *
     *  Return a data type for each column such that every value in a column can be
     *  converted to the corresponding data type without data loss.
     *  @param[in]  filename The CSV file
     *
     *  \return A mapping of column names to csv::DataType enums
     */
    CSV_INLINE std::unordered_map<std::string, DataType> csv_data_types(const std::string& filename) {
        CSVStat stat(filename);
        std::unordered_map<std::string, DataType> csv_dtypes;

        auto& col_names = stat.get_col_names();
        const size_t count = col_names.size();
        auto temp = stat.get_dtypes();

        for (size_t i = 0; i < count; i++) {
            auto& col = temp[i];
            auto& col_name = col_names[i];

            if (col[DataType::CSV_STRING])
                csv_dtypes[col_name] = DataType::CSV_STRING;
            else if (col[DataType::CSV_INT64])
                csv_dtypes[col_name] = DataType::CSV_INT64;
            else if (col[DataType::CSV_INT32])
                csv_dtypes[col_name] = DataType::CSV_INT32;
            else if (col[DataType::CSV_INT16])
                csv_dtypes[col_name] = DataType::CSV_INT16;
            else if (col[DataType::CSV_INT8])
                csv_dtypes[col_name] = DataType::CSV_INT8;
            else
                csv_dtypes[col_name] = DataType::CSV_DOUBLE;
        }

        return csv_dtypes;
    }
}