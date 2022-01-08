#pragma once
#include "common.hpp"
#include "csv_format.hpp"
#include "csv_reader.hpp"
#include "data_type.h"

#include <string>
#include <type_traits>
#include <unordered_map>

namespace csv {
    /** Returned by get_file_info() */
    struct CSVFileInfo {
        std::string filename;               /**< Filename */
        std::vector<std::string> col_names; /**< CSV column names */
        char delim;                         /**< Delimiting character */
        size_t n_rows;                      /**< Number of rows in a file */
        size_t n_cols;                      /**< Number of columns in a CSV */
    };

    /** @name Shorthand Parsing Functions
     *  @brief Convenience functions for parsing small strings
     */
     ///@{
    CSVReader operator ""_csv(const char*, size_t);
    CSVReader operator ""_csv_no_header(const char*, size_t);
    CSVReader parse(csv::string_view in, CSVFormat format = CSVFormat());
    CSVReader parse(std::istream& stream, CSVFormat format = CSVFormat());
    CSVReader parse_no_header(csv::string_view in);
    CSVReader parse_no_header(std::istream& stream);

    /** Shorthand function for parsing streams
     *
     *  @return A collection of CSVRow objects
     *
     *  @par Example
     *  @snippet tests/test_read_csv.cpp Parse Example
     */
    template<typename TStream,
        csv::enable_if_t<std::is_base_of<std::istream, TStream>::value, int> = 0>
    CSVReader parse(TStream& source, CSVFormat format) {
        return CSVReader(stream, format);
    }

    template<typename TStream,
        csv::enable_if_t<std::is_base_of<std::istream, TStream>::value, int> = 0>
    CSVReader parse_no_header(std::istream& stream) {
        CSVFormat format;
        format.header_row(-1);

        return parse(stream, format);
    }
    ///@}

    /** @name Utility Functions */
    ///@{
    std::unordered_map<std::string, DataType> csv_data_types(const std::string&);
    CSVFileInfo get_file_info(const std::string& filename);
    int get_col_pos(csv::string_view filename, csv::string_view col_name,
        const CSVFormat& format = CSVFormat::guess_csv());
    ///@}
}