#pragma once

#include <iterator>

template <typename T>
class CountedRange
{
public:
    CountedRange() = default;
    CountedRange(T end) : m_end(end) {}
    CountedRange(T begin, T end) : m_begin(begin), m_end(end) {}
    CountedRange(const CountedRange&) = default;
    CountedRange(CountedRange&&) = default;
    CountedRange& operator=(const CountedRange& rhs) { m_begin = rhs.m_begin; m_end = rhs.m_end; return *this; }
    CountedRange& operator=(CountedRange&& rhs) { m_begin = rhs.m_begin; m_end = rhs.m_end; return *this; }

    struct iterator
    {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = T;
        using pointer = const value_type*;
        using reference = const value_type&;

        iterator() = default;
        iterator(value_type current) : m_current(current) {}
        iterator(const iterator&) = default;
        iterator(iterator&&) = default;
        iterator& operator=(const iterator& rhs) { m_current = rhs.m_current; return *this; }
        iterator& operator=(iterator&& rhs) { m_current = rhs.m_current; return *this; }

        reference operator*() const { return m_current; }
        pointer operator->() { return &m_current; }

        // Prefix increment
        iterator& operator++() { m_current++; return *this; }

        // Postfix increment
        iterator operator++(int) { iterator tmp = *this; ++(*this); return tmp; }

        friend bool operator== (const iterator& a, const iterator& b) { return a.m_current == b.m_current; };
        friend bool operator!= (const iterator& a, const iterator& b) { return a.m_current != b.m_current; };

    private:

        value_type m_current{};
    };

    iterator begin() const { return iterator(m_begin); }
    iterator end() const { return iterator(m_end); }

private:
    const T m_begin{};
    const T m_end{};
};
