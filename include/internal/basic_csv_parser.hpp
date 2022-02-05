/** @file
 *  @brief Contains the main CSV parsing algorithm and various utility functions
 */

#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <fstream>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "../external/mio.hpp"
#include "../external/readerwritercircularbuffer.h"
#include "col_names.hpp"
#include "common.hpp"
#include "csv_format.hpp"
#include "csv_row.hpp"

namespace csv {

    using ChannelBuffer = moodycamel::BlockingReaderWriterCircularBuffer<CSVRow>;

    // 
    /** Read rows produced by IBasicCSVParser. 
     *
     *  Intended to be called from the primary thread.
     */
    class CSVQueueReader {
    public:

        CSVQueueReader(ChannelBuffer& channel, std::future<bool>&& bomResult)
            : _channel(channel), _bomResult(std::move(bomResult)) {}

        /** @name Retrieving CSV Rows */
        ///@{

        bool read_row(CSVRow& row);

        /** Read all rows and write to an inserter.
         */
        template <class T>
        void read_collection(T inserter)
        {
            CSVRow row;
            while (this->read_row(row))
            {
                *inserter++ = std::move(row);
            }
        }

        /** True if all rows have been read.
         */
        bool eof() { return this->_received_tombstone; }

        /** @name CSV Metadata: Attributes */
        ///@{

        /** Retrieves the number of rows that have been read so far */
        CONSTEXPR size_t n_rows() const noexcept { return this->_n_rows; }

        /** Whether or not CSV was prefixed with a UTF-8 bom */
        bool utf8_bom() const {
            return this->_bomResult.get();
        }
        ///@}

        /** Low level access to read queue. You may not call any reader functions if you call any functions
            that change the queue. Make sure you handle tombstones! */
        ChannelBuffer& channel() { return _channel; }

        void set_current_exception() {
            this->teptr = std::current_exception();
        }

        bool _utf8_bom = false;

    protected:

        ChannelBuffer& _channel;

        mutable std::future<bool> _bomResult;

        size_t _n_rows = 0; /**< How many rows (minus header) have been read so far */

        std::exception_ptr teptr = nullptr;

        bool _received_tombstone = false;
    };

    namespace internals {
        std::string format_row(const std::vector<std::string>& row, csv::string_view delim = ", ");

        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
        HEDLEY_CONST CONSTEXPR_17 ParseFlagMap make_parse_flags(char delimiter) {
            std::array<ParseFlags, 256> ret = {};
            for (int i = -128; i < 128; i++) {
                const int arr_idx = i + 128;
                char ch = char(i);

                if (ch == delimiter)
                    ret[arr_idx] = ParseFlags::DELIMITER;
                else if (ch == '\r' || ch == '\n')
                    ret[arr_idx] = ParseFlags::NEWLINE;
                else
                    ret[arr_idx] = ParseFlags::NOT_SPECIAL;
            }

            return ret;
        }

        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character and, v[i + 128] labels it according to
         *  the CSVReader::ParseFlags enum
         */
        HEDLEY_CONST CONSTEXPR_17 ParseFlagMap make_parse_flags(char delimiter, char quote_char) {
            std::array<ParseFlags, 256> ret = make_parse_flags(delimiter);
            ret[(size_t)quote_char + 128] = ParseFlags::QUOTE;
            return ret;
        }

        /** Create a vector v where each index i corresponds to the
         *  ASCII number for a character c and, v[i + 128] is true if
         *  c is a whitespace character
         */
        HEDLEY_CONST CONSTEXPR_17 WhitespaceMap make_ws_flags(const char* ws_chars, size_t n_chars) {
            std::array<bool, 256> ret = {};
            for (int i = -128; i < 128; i++) {
                const int arr_idx = i + 128;
                char ch = char(i);
                ret[arr_idx] = false;

                for (size_t j = 0; j < n_chars; j++) {
                    if (ws_chars[j] == ch) {
                        ret[arr_idx] = true;
                    }
                }
            }

            return ret;
        }

        inline WhitespaceMap make_ws_flags(const std::vector<char>& flags) {
            return make_ws_flags(flags.data(), flags.size());
        }

        CSV_INLINE size_t get_file_size(csv::string_view filename);

        CSV_INLINE std::string get_csv_head(csv::string_view filename);

        /** Read the first 500KB of a CSV file */
        CSV_INLINE std::string get_csv_head(csv::string_view filename, size_t file_size);

        constexpr const int UNINITIALIZED_FIELD = -1;
    }

    namespace internals {
        /** Abstract base class which provides CSV parsing logic.
         * 
         *  Safe to access from the primary thread:
         *    - reader
         *    - stop_thread_and_join()
         *
         *  Concrete implementations may customize this logic across
         *  different input sources, such as memory mapped files, stringstreams,
         *  etc...
         */
        class IBasicCSVParser {
        public:
            IBasicCSVParser() = default;
            IBasicCSVParser(const CSVFormat&, const ColNamesPtr&);
            IBasicCSVParser(const ParseFlagMap& parse_flags, const WhitespaceMap& ws_flags
            ) : _parse_flags(parse_flags), _ws_flags(ws_flags) {};

            virtual ~IBasicCSVParser() = default;

            /** Main loop to read the CSV file. Okay to call this directly
                for a string or stream-based csv file, but do not call this
                directly for very large memory-mapped (file-based) CSV files
                or the whole file will be memory-mapped, which can fail.
             */
            bool read_csv(size_t bytes = static_cast<size_t>(~0));

            void stop_thread_and_join(std::thread& read_csv_worker);

            void set_max_rows(size_t max_rows = -1) {
                _max_rows = max_rows;
            }

            void set_iteration_chunk_size(size_t chunk_size) {
                this->_iteration_chunk_size = chunk_size;
            }

            std::promise<bool> _bomPromise;

            ChannelBuffer _channel = ChannelBuffer(1000);

            CSVQueueReader queue_reader = CSVQueueReader(_channel, std::move(_bomPromise.get_future()));

            std::atomic_bool _terminateNow;

        protected:
            /** Parse the next block of data */
            virtual void next() = 0;

            /** Indicate the last block of data has been parsed */
            void end_feed();

            /** Used by worker thread to put a tombstone on the queue */
            void enqueue_tombstone();

            CONSTEXPR_17 ParseFlags parse_flag(const char ch) const noexcept {
                return _parse_flags.data()[ch + 128];
            }

            CONSTEXPR_17 ParseFlags compound_parse_flag(const char ch) const noexcept {
                return quote_escape_flag(parse_flag(ch), this->quote_escape);
            }

            /** Whether or not we have reached the end of source. This must not be used
                from the primary thread because it is updated asynchronously.
             */
            bool eof() { return this->_eof; }

            /** Whether or not source needs to be read in chunks */
            CONSTEXPR bool no_chunk() const { return this->source_size < this->_iteration_chunk_size; }

            /** Parse the current chunk of data *
             *
             *  @returns How many character were read that are part of complete rows
             */
            size_t parse(bool forceNewline);

            /** Create a new RawCSVDataPtr for a new chunk of data */
            void reset_data_ptr();

            /** Handle possible Unicode byte order mark */
            void trim_utf8_bom();

        protected:
            /** @name Current Parser State */
            ///@{
            CSVRow current_row;
            // TODO - not thread safe!
            RawCSVDataPtr data_ptr;
            ColNamesPtr _col_names;
            //CSVFieldList* fields = nullptr;
            int field_start = UNINITIALIZED_FIELD;
            size_t field_length = 0;
            size_t _max_rows = (size_t)~0;    // Read no more than this many lines

            /** An array where the (i + 128)th slot gives the ParseFlags for ASCII character i */
            ParseFlagMap _parse_flags;
            ///@}

            /** @name Current Stream/File State */
            ///@{
            bool _eof = false;

            /** The size of the incoming CSV */
            size_t source_size = 0;

            size_t _iteration_chunk_size = ITERATION_CHUNK_SIZE;
            ///@}

        private:
            /** An array where the (i + 128)th slot determines whether ASCII character i should
             *  be trimmed
             */
            WhitespaceMap _ws_flags;
            bool quote_escape = false;
            bool field_has_double_quote = false;

            /** Where we are in the current data block */
            size_t data_pos = 0;

            /** Whether or not an attempt to find Unicode BOM has been made */
            bool unicode_bom_scan = false;

            CONSTEXPR_17 bool ws_flag(const char ch) const noexcept {
                return _ws_flags.data()[ch + 128];
            }

            size_t& current_row_start() {
                return this->current_row.data_start;
            }

            /** Process a newline during parsing */
            void processNewline();

            void parse_field() noexcept;

            /** Finish parsing the current field */
            void push_field();

            /** Finish parsing the current row */
            void push_row();
        };

        /** A class for parsing CSV data from a `std::stringstream`
         *  or an `std::ifstream`
         */
        template<typename TStream>
        class StreamParser: public IBasicCSVParser {
        public:
            StreamParser(TStream& source,
                const CSVFormat& format,
                const ColNamesPtr& col_names = nullptr
            ) : IBasicCSVParser(format, col_names), _source(std::move(source)) {};

            StreamParser(
                TStream& source,
                internals::ParseFlagMap parse_flags,
                internals::WhitespaceMap ws_flags) :
                IBasicCSVParser(parse_flags, ws_flags),
                _source(std::move(source))
            {};

            ~StreamParser() = default;

        protected:
            void next() override {
                if (this->eof()) return;

                this->reset_data_ptr();

                if (source_size == 0) {
                    const auto start = _source.tellg();
                    _source.seekg(0, std::ios::end);
                    const auto end = _source.tellg();
                    _source.seekg(0, std::ios::beg);

                    source_size = end - start;
                }

                // Read data into buffer
                size_t length = std::min(source_size - stream_pos, this->_iteration_chunk_size);
                stream_pos = this->data_ptr->CreateStringSource(_source, stream_pos, length);
                const bool forceNewline = (stream_pos + this->_iteration_chunk_size >= source_size);

                // Parse
                this->current_row = CSVRow(this->data_ptr);
                size_t completed = this->parse(forceNewline);
                this->stream_pos -= (length - completed);

                if (eof() || stream_pos == source_size || no_chunk()) {
                    this->_eof = true;
                    this->end_feed();
                }
            }

        private:
            TStream _source;
            size_t stream_pos = 0;
        };

        /** A class for parsing CSV data from a `std::stringstream`
         *  or an `std::ifstream`
         */
        class StringViewParser : public IBasicCSVParser {
        public:
            StringViewParser(std::string_view source,
                const CSVFormat& format,
                const ColNamesPtr& col_names = nullptr
            ) : IBasicCSVParser(format, col_names), _source(std::move(source)) {};

            //StringViewParser(
            //    std::string_view& source,
            //    internals::ParseFlagMap parse_flags,
            //    internals::WhitespaceMap ws_flags) :
            //    IBasicCSVParser(parse_flags, ws_flags),
            //    _source(std::move(source))
            //{};

        protected:
            CSV_INLINE void next() override;

        private:
            std::string_view _source;
            size_t stream_pos = 0;
        };

        /** Parser for memory-mapped files
         *
         *  @par Implementation
         *  This class constructs moving windows over a file to avoid
         *  creating massive memory maps which may require more RAM
         *  than the user has available. It contains logic to automatically
         *  re-align each memory map to the beginning of a CSV row.
         *
         */
        class MmapParser : public IBasicCSVParser {
        public:
            MmapParser(csv::string_view filename,
                const CSVFormat& format,
                const ColNamesPtr& col_names = nullptr
            ) : IBasicCSVParser(format, col_names) {
                this->_filename = filename.data();
                this->source_size = get_file_size(filename);
            };

            ~MmapParser() = default;

        protected:
            void next() override;

        private:
            std::string _filename;
            size_t mmap_pos = 0;
        };
    }
}
