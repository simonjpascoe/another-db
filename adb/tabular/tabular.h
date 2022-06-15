#pragma once

#include <numeric>
#include <map>
#include <variant>

#include "central.h"
#include "schema.h"

class Tabular; 

typedef std::variant<std::monostate, int, std::string, double> Entry;
typedef std::shared_ptr<const Tabular> PT;
typedef std::vector<Entry> ES;

// aggregations temporary
//const double sum(std::vector<double> input)
//{
//    return std::accumulate(input.begin(), input.end(), 0.0);
//}

class CategoricalStore
{
private:
    int next_index = 0;
    std::map<std::string, int> m_ab;
    std::map<int, Entry> m_ba;

public:
    CategoricalStore()
    {
    }

    int operator[](std::string value)
    {
        auto search = m_ab.find(value);
        if (search != m_ab.end()) {
            return search->second;
        }
        else
        {
            auto k = next_index++;
            m_ab[value] = k;
            m_ba[k] = Entry(value);
            return k;
        }
    }

    const Entry& operator[](int value)
    {
        return m_ba[value];
    }

    const std::vector<int> store(std::vector<std::string> strings)
    {
        std::vector<int> output;
        std::transform(strings.begin(), strings.end(), std::back_inserter(output), 
            [this](std::string s) -> int { return operator[](s); });
        return output;
    }
};

class ColumnShard
{
private:
    const DataType m_dt;
    const std::shared_ptr<CategoricalStore> m_cstore;
    std::vector<Entry> m_entries;

public:
    ColumnShard(const std::vector<int> ints) : m_dt(DataType::INT)
    {
        std::transform(ints.begin(), ints.end(), std::back_inserter(m_entries),
            [](int i) -> Entry { return Entry(i); });
    }

    ColumnShard(const std::vector<std::optional<int>> ints) : m_dt(DataType::INT)
    {
        std::transform(ints.begin(), ints.end(), std::back_inserter(m_entries),
            [](std::optional<int> i) -> Entry { return i.has_value() ? Entry(*i) : Entry(std::monostate()); });
    }

    ColumnShard(const std::vector<double> doubles) : m_dt(DataType::DOUBLE)
    {
        std::transform(doubles.begin(), doubles.end(), std::back_inserter(m_entries),
            [](double d) -> Entry { return Entry(d); });
    }
    
    ColumnShard(const std::vector<std::optional<double>> doubles) : m_dt(DataType::DOUBLE)
    {
        std::transform(doubles.begin(), doubles.end(), std::back_inserter(m_entries),
            [](std::optional<double> i) -> Entry { return i.has_value() ? Entry(*i) : Entry(); });
    }

    ColumnShard(const std::vector<std::string> strings) : m_dt(DataType::TEXT)
    {
        std::transform(strings.begin(), strings.end(), std::back_inserter(m_entries),
            [](std::string s) -> Entry { return Entry(s); });
    }

    ColumnShard(const std::vector<std::string> strings,
        const std::shared_ptr<CategoricalStore> categoricalStore):
        m_dt(DataType::SHORT_TEXT), m_cstore(categoricalStore)
    {
        auto inserted = categoricalStore->store(strings);
        std::transform(inserted.begin(), inserted.end(), std::back_inserter(m_entries),
            [](int d) -> Entry { return Entry(d); });
    }

    const std::size_t nRows() const
    {
        return m_entries.size();
    }

    const Entry& operator[](std::size_t n) const
    {
        if (m_dt == DataType::SHORT_TEXT)
            return m_cstore->operator[](std::get<int>(m_entries[n]));
        else
            return m_entries[n];
    }
};

class Tabular : public std::enable_shared_from_this<Tabular>
{
public:

    static const PT createFromColumns(
        const Schema& schema,
        const std::vector<ColumnShard> columns);

    virtual const Schema& getSchema() const = 0;
    virtual const std::size_t nRows() const = 0;
    //virtual const Column& operator[](std::size_t column) const = 0;

    virtual const Entry& getValue(std::size_t row, std::size_t column) const = 0;

    const std::size_t nCols() const
    {
        return getSchema().columns().size();
    }

    void basicPrint() const;

    const PT project(const CS columns) const;
    const PT rename(const CS original, const CS renamed) const;
    const PT concat(const PT other, bool horizontal) const;
    const PT join(const PT other, const CS keys1, const CS keys2) const;
    const PT groupBy(const CS keys, const AS aggrs) const;
    const PT filter(const CS columns, const ES values) const;
};


class Index
{
private:
    const CS m_columns;
    std::map<std::vector<Entry>, std::vector<std::size_t>> m_groups;

public:
    Index(const PT& source, const CS& columns) :
        m_columns(columns)
    {
        for (std::size_t i = 0; i < source->nRows(); ++i)
        {
            ES key;
            for (const std::string& c : columns)
                key.push_back(source->getValue(i, source->getSchema().indexOf(c)));

            auto existing = m_groups.find(key);
            if (existing == m_groups.end())
            {
                m_groups[key] = std::vector<std::size_t>({ i });
            }
            else
            {
                existing->second.push_back(i);
            }
        }
    }

    const std::map<ES, std::vector<std::size_t>>& groups() const
    {
        return m_groups;
    }

    const std::vector<std::size_t> indicies(const std::string column, const Entry value) const;
};



class Table0 : public Tabular
{
private:
    const CategoricalStore m_cstore;
    const std::vector<ColumnShard> m_columns;
    const Schema m_schema;

public:
    Table0(const Schema& schema, const std::vector<ColumnShard>& columns):
        m_schema(schema), m_columns(columns)
    {}

    const Schema& getSchema() const
    {
        return m_schema;
    }

    const std::size_t nRows() const
    {
        return m_columns[0].nRows();
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class Table : public Tabular
{
protected:
    const std::vector<PT> m_underlyings;
    const Schema m_schema;

public:
    Table(const std::vector<PT>& underlyings, const Schema& schema) :
        m_underlyings(underlyings), m_schema(schema)
    {

    }

    const std::size_t nRows() const
    {
        return m_underlyings[0]->nRows();
    }

    const Schema& getSchema() const
    {
        return m_schema;
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class Project : public Table
{
private:
    std::map<std::size_t, std::size_t> m_colmap;

public:
    Project(const PT underlying, const CS& columns):
        Table({ underlying }, underlying->getSchema().project(columns))
    {
        auto schema0 = underlying->getSchema();
        // also build our column map from new -> old
        for (auto i = 0; i < columns.size(); ++i)
        {
            m_colmap[i] = schema0.indexOf(columns[i]);
        }
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class Rename : public Table
{

public:
    Rename(const PT underlying,
        const CS& original,
        const CS& renamed) :
        Table({ underlying }, underlying->getSchema().rename(original, renamed))
    {
    }
};


class Concat : public Table
{
private:
    bool m_horizontal;

public:
    Concat(const PT underlying1,
        const PT underlying2,
        bool horizontal) :
        Table({ underlying1, underlying2 }, 
            horizontal ? underlying1->getSchema().hconcat(underlying2->getSchema()) : underlying1->getSchema()),
        m_horizontal(horizontal)
    {

    }

    virtual const std::size_t nRows() const;
    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class Join : public Table
{
private:
    const CS m_keys1;
    const CS m_keys2;

    std::vector<std::pair<std::size_t, std::size_t>> m_rowindex;
    std::map<std::size_t, std::size_t> m_other_colindex;

public:
    Join(const PT underlying1,
        const PT underlying2,
        const CS& keys1,
        const CS& keys2):
        Table({underlying1, underlying2},
            underlying1->getSchema().join(underlying2->getSchema(), keys1, keys2)),
        m_keys1(keys1), m_keys2(keys2)
    {
        // todo: revisit for better impl later?
        auto s = getSchema();
        auto so = underlying2->getSchema();
        for (auto c : so.columns())
        {
            auto nm = c.getName();
            if (s.has_column(nm))
            {
                m_other_colindex[s.indexOf(nm)] = so.indexOf(nm);
            }
        }
        
        auto index1 = Index(underlying1, keys1);
        auto index2 = Index(underlying2, keys2);

        // here is where the different joins operate
        // first test is a inner left join
        auto g2s = index2.groups();
        for (auto g1 : index1.groups())
        {
            // do we have a matching group in index2?
            auto other = g2s.find(g1.first);
            if (other != g2s.end())
            {
                for (auto r1 : g1.second)
                    for (auto r2 : other->second)
                        m_rowindex.push_back(std::pair(r1, r2));
            }
        }
    }

    virtual const std::size_t nRows() const
    {
        return m_rowindex.size();
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class GroupBy : public Table
{
private:
    const CS m_keys;
    const AS m_aggrs;
    const Index m_index;

    // performance cache, beware mutable
    mutable std::vector<std::vector<std::optional<Entry>> > m_cache;
 
public:
    GroupBy(const PT underlying,
        const CS& keys,
        const AS& aggrs) :
        Table({ underlying }, underlying->getSchema().groupBy(keys, aggrs)),
        m_keys(keys), m_aggrs(aggrs), m_index(Index(underlying, keys))
    {

    }

    virtual const std::size_t nRows() const
    {
        return m_index.groups().size();
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};

class Filter1 : public Table
{
private:
    const CS m_columns;
    const ES m_values;
    std::vector<std::size_t> m_rowdex;

public:
    Filter1(const PT underlying,
        const CS& columns,
        const ES& values)
        : Table({underlying}, underlying->getSchema()),
        m_columns(columns), m_values(values)
    {
        // since it is an AND condition, we can scan each individually
        // and then just keep the final result
        std::vector<std::size_t> index(underlying->nRows());
        std::iota(index.begin(), index.end(), 0);

        for (auto i = 0; i < columns.size(); ++i)
        {
            std::vector<std::size_t> index2;
            auto c = m_schema.indexOf(columns[i]);
            for (auto j = 0; j < index.size(); ++j)
            {
                if (underlying->getValue(index[j], c) == values[i])
                {
                    index2.push_back(index[j]);
                }
            }
            index = index2;
        }

        m_rowdex = index;
    }

    virtual const std::size_t nRows() const
    {
        return m_rowdex.size();
    }

    virtual const Entry& getValue(std::size_t row, std::size_t column) const;
};