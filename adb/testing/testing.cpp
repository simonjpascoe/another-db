// testing.cpp : Defines the entry point for the application.
//

#include "testing.h"
#include "tabular.h"
#include "schema.h"

using namespace std;

int main()
{
    cout << ">>> Hello from adb." << endl;

    vector<ColumnDefinition> v1 = {
        ColumnDefinition("a", DataType::INT),
        ColumnDefinition("c", DataType::SHORT_TEXT)
    };
    
    vector<ColumnDefinition> v2 = {
     ColumnDefinition("A", DataType::INT),
     ColumnDefinition("C", DataType::SHORT_TEXT),
     ColumnDefinition("D", DataType::DOUBLE),
     ColumnDefinition("E", DataType::SHORT_TEXT),
    };

    CategoricalStore cs;
    auto csp = std::make_shared<CategoricalStore>(cs);

    Schema s1(v1);
    Schema s2(v2);

    std::vector<int> a{ 1,1,3 };
    std::vector<std::string> c{ "one", "two", "six"};

    std::vector<int> A{ 4,1,3,3 };
    std::vector<std::string> C {"four", "one", "six", "six" };
    std::vector<double> D{ 0.22, 0.34, 1.23, 3.33};
    std::vector<std::string> E{ "alpha", "beta", "delta", "gamma" };

    auto col_a = ColumnShard(a);
    auto col_c = ColumnShard(c);

    auto col_A = ColumnShard(A);
    auto col_C = ColumnShard(C);
    auto col_D = ColumnShard(D);
    auto col_E = ColumnShard(E);
    
    auto t = Tabular::createFromColumns(s1, { col_a, col_c });
    auto u = Tabular::createFromColumns(s2, { col_A, col_C, col_D, col_E });


    t->basicPrint();
    cout << endl;
    u->basicPrint();
    cout << endl;

    auto w = t->join(u, { "a", "c" }, { "A", "C" });
    w->basicPrint();
    cout << endl;

    auto k = w->groupBy({ "a" }, { std::make_tuple(Aggr::uSum, "D", std::nullopt),
                                   std::make_tuple(Aggr::uFirst, "D", std::nullopt),
                                   std::make_tuple(Aggr::uLast, "E", std::nullopt) });
    k->basicPrint();

    return 0;
}
