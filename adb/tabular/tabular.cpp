#include <iostream>

#include "tabular.h"


const std::vector<std::size_t> Index::indicies(const std::string column, const Entry value) const
{
    std::size_t idx = std::distance(m_columns.begin(), std::find(m_columns.begin(), m_columns.end(), column));
    std::vector<std::size_t> indicies;
    for (auto g : m_groups)
    {
        if (g.first[idx] == value)
        {
            indicies.insert(indicies.end(), g.second.begin(), g.second.end());
        }
    }

    std::sort(indicies.begin(), indicies.end());
    return indicies;
}

const PT Tabular::createFromColumns(
    const Schema& schema,
    const std::vector<ColumnShard> columns)
{
    return std::make_shared<Table0>(schema, columns);
}

void Tabular::basicPrint() const
{
    auto schema = getSchema();

    std::cout << "| ";
    for (auto c : schema.columns())
    {
        std::cout << c.getName() << " | ";
    }
    std::cout << std::endl;

    for (auto i = 0; i < nRows(); ++i)
    {
        std::cout << "| ";
        for (auto j = 0; j < nCols(); ++j)
        {
            auto v = getValue(i, j);
            if (v.has_value())
            {
                auto index = v->index();
                switch (index)
                {
                case 0:
                    std::cout << std::get<int>(*v);
                    break;
                case 1:
                    std::cout << std::get<std::string>(*v);
                    break;
                case 2:
                    std::cout << std::get<double>(*v);
                }
            }
            else
            {
                std::cout << " -- ";
            }
            
            std::cout << " | ";
        }
        std::cout << std::endl;
    }
}

const Entry Table0::getValue(std::size_t row, std::size_t column) const
{
    return m_columns[column][row];
}

const Entry Table::getValue(std::size_t row, std::size_t column) const
{
    return m_underlyings[0]->getValue(row, column);
}

const PT Tabular::project(const CS columns) const
{
    return std::make_shared<Project>(shared_from_this(), columns);
}

const Entry Project::getValue(std::size_t row, std::size_t column) const
{
    return m_underlyings[0]->getValue(row, m_colmap.at(column));
}

const PT Tabular::rename(
    const CS original, const CS renamed) const
{
    return std::make_shared<Rename>(shared_from_this(), original, renamed);
}

const PT Tabular::concat(
    const PT other, bool horizontal) const
{
    return std::make_shared<Concat>(shared_from_this(), other, horizontal);
}

const std::size_t Concat::nRows() const
{
    if (m_horizontal)
        return m_underlyings[0]->nRows();
    else
        return m_underlyings[0]->nRows() + m_underlyings[1]->nRows();
}

const Entry Concat::getValue(std::size_t row, std::size_t column) const
{
    auto sz = m_underlyings[0]->nRows();
    auto sy = m_underlyings[0]->nCols();

    if (m_horizontal)
    {
        if (column >= sy)
            return m_underlyings[1]->getValue(row, column-sy);
        else
            return m_underlyings[0]->getValue(row, column);
    }
    else
    {
        if (row >= sz)
            return m_underlyings[1]->getValue(row - sz, column);
        else
            return m_underlyings[0]->getValue(row, column);

    }
}

const PT Tabular::join(const PT other, const CS keys1, const CS keys2) const
{
    return std::make_shared<Join>(shared_from_this(), other, keys1, keys2);
}

const Entry Join::getValue(std::size_t row, std::size_t column) const
{
    auto p = m_rowindex[row];
    auto sy = m_underlyings[0]->nCols();

    // for now, join does not remove columns from the left table
    if (column < sy)
    {
        return m_underlyings[0]->getValue(p.first, column);
    }
    else
    {
        return m_underlyings[1]->getValue(p.second, m_other_colindex.at(column));
    }
}

const PT Tabular::groupBy(const CS keys, const AS aggrs) const
{
    return std::make_shared<GroupBy>(shared_from_this(), keys, aggrs);
}

const Entry GroupBy::getValue(std::size_t row, std::size_t column) const
{
    auto it = m_index.groups().begin();
    std::advance(it, row);
    if (column < m_keys.size()) {
        return it->first[column];
    }
    else
    {
        // calculate the source column
        std::size_t offset = column - m_keys.size();
        std::size_t source = m_underlyings[0]->getSchema().indexOf(std::get<1>(m_aggrs[offset]));

        switch (std::get<0>(m_aggrs[offset]))
        {
            case Aggr::uSum:
                return std::accumulate(it->second.begin(), it->second.end(), 0.0,
                    [this, source](double s, std::size_t j) -> double {return s + std::get<double>(*m_underlyings[0]->getValue(j, source)); });
            case Aggr::uFirst:
                return m_underlyings[0]->getValue(*(it->second.begin()), source);
            case Aggr::uLast:
                return m_underlyings[0]->getValue(*(it->second.end()-1), source);
        }
    }
}

const PT Tabular::filter(const CS columns, const ES values) const
{
    return std::make_shared<Filter1>(shared_from_this(), columns, values);
}

const Entry Filter1::getValue(std::size_t row, std::size_t column) const
{
    return m_underlyings[0]->getValue(m_rowdex[row], column);
}