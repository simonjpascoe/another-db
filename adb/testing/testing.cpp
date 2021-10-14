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
     ColumnDefinition("C", DataType::SHORT_TEXT)
    };

    CategoricalStore cs;
    auto csp = std::make_shared<CategoricalStore>(cs);

    Schema s1(v1);
    Schema s2(v2);

    std::vector<int> a{ 1,1,3 };
    std::vector<std::string> c{ "one", "two", "three"};

    std::vector<int> A{ 4,1,3 };
    std::vector<std::string> C{ "four", "five", "six" };

    auto col_a = ColumnShard(a);
    auto col_c = ColumnShard(c);

    auto col_A = ColumnShard(A);
    auto col_C = ColumnShard(C);
    
    auto t = Tabular::createFromColumns(s1, { col_a, col_c });
    auto u = Tabular::createFromColumns(s2, { col_A, col_C });


    t->basicPrint();
    cout << endl;
    u->basicPrint();
    cout << endl;

    auto w = t->join(u, { "a" }, { "A" })->rename({ "C" }, { "little_c" });
    w->basicPrint();

    return 0;
}
