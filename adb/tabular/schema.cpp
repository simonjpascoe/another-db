#include "central.h"
#include "schema.h"


const Schema Schema::project(const CS& columns) const
{
    std::vector<ColumnDefinition> cols;
    for (auto c : columns)
        cols.push_back(m_columns[indexOf(c)]);

    return Schema(cols);
}

const Schema Schema::rename(const CS& original, const CS& renamed) const
{
    std::vector<ColumnDefinition> cols(m_columns);
    for (auto i = 0; i < original.size(); ++i)
    {
        auto index = indexOf(original[i]);
        cols[index] = cols[index].rename(renamed[i]);
    }

    return Schema(cols);
}

const Schema Schema::hconcat(const Schema& other) const
{
    auto sz = m_columns.size() + other.m_columns.size();
    std::vector<ColumnDefinition> cols;
    cols.reserve(sz);
    
    auto it = cols.begin();
    std::copy(m_columns.begin(), m_columns.end(), it);
    std::copy(m_columns.begin(), m_columns.end(), it);

    return Schema(cols);
}

const Schema Schema::join(const Schema& other, const CS& keys1, const CS& keys2) const
{
    // basic lefty join for the prototype
    // keys are auto merged to the left nameing convention
    // all columns are included

    std::vector<ColumnDefinition> cols;

    std::copy(m_columns.begin(), m_columns.end(), std::back_inserter(cols));
    std::copy_if(other.m_columns.begin(), other.m_columns.end(), std::back_inserter(cols),
        [keys2](ColumnDefinition cd) -> bool { return not(std::any_of(keys2.begin(), keys2.end(), [cd](std::string s) -> bool { return s == cd.getName(); })); });

    return Schema(cols);

}