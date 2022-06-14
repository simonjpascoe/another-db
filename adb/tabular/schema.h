#pragma once

#include <map>

#include "central.h"

enum class DataType {
    SHORT_TEXT,
    TEXT,
    INT,
    DOUBLE
};

class ColumnDefinition
{
private:
    std::string m_name;
    DataType m_datatype;

public:
    ColumnDefinition(const std::string name, const DataType datatype):
        m_name(name), m_datatype(datatype)
    {}

    const std::string getName() const
    {
        return m_name;
    }

    const DataType getDataType() const
    {
        return m_datatype;
    }

    const ColumnDefinition rename(const std::string name) const
    {
        return ColumnDefinition(name, m_datatype);
    }
};

class Schema
{
private:
    std::vector<ColumnDefinition> m_columns;
    std::map<std::string, std::size_t> m_index;

public:
    Schema(std::vector<ColumnDefinition> columns = {}) :
        m_columns(columns)
    {
        for (auto i = 0; i < m_columns.size(); ++i)
            m_index[m_columns[i].getName()] = i;
    }

    bool has_column(const std::string& name)
    {
        return m_index.find(name) != m_index.end();
    }

    const std::vector<ColumnDefinition>& columns() const
    {
        return m_columns;
    }

    const std::size_t indexOf(const std::string& name) const
    {
        return m_index.at(name);
    }

    const Schema project(const CS& columns) const;
    const Schema rename(const CS& original, const CS& renamed) const;
    const Schema hconcat(const Schema& other) const;
    const Schema join(const Schema& other, const CS& keys1, const CS& keys2) const;
    const Schema groupBy(const CS& keys, const AS& aggrs) const;
};