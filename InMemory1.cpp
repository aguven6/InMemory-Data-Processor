/*
In Memory Data Processing
-----------------------------------------------------------------------------------------------------------------

Welcome here! Purpose of this study is create a columnar data structre from tabular data structures. Program will
fetch data from a tabular data source and create will vectors for each column with indexes.

Let'a assume we have a csv file that includes following columns: Name,Surname,Age,BirthDate

We will create 4 vectors for each columns like:

Index is an autoincremental number

Vector1: Index,Name
Vector2: Index,Surname
Vector3: Index,Age
Vector4: Index,Birthdate

Tasks
-----------------------------------------------------------------------------------------------------------------
Filter Data
----------------------------------------------------
1- Query By Single Value for a Column               Where Name="Ahmet"
2- Query By Multiple Values for a Column            Where Name="Ahmet" or Name="Metin"
3- Query Multiple Columns with Single Values        Where Name="Ahmet" and Surname="Mehmet"
4- Query Multiple Columns with Multiple Values      Where Name="Ahmet" or Name="Metin" and Age >30 and Age <50

Aggregation
----------------------------------------------------
1- Calculate  Sum,Count,Avg, Max, Min for whole data.
2- Calculate  Sum,Count,Avg, Max, Min with Filtering.
3- Group By a Key and aggregate values for whole data
4- Group By a Key and aggregate values with Filtering
*/

#include <iostream>
#include <vector>
#include <any>
#include <typeinfo>
#include<variant>
#include <algorithm>

using namespace std;

//Data Item to store Data with Index of the Data
template<typename T>
class IndexedData {
public:
    IndexedData<T>(int index,T value) 
    {
        this->index = index;
        this->value = value;
    };
    int index;  // Row index
    T value;    // Data value

};

class InMemoryTable
{
public:
    InMemoryTable() {}
    template<typename T>
    void LoadVectorData(vector<IndexedData<T>> Data)
    {
        //Sort Ascending Order
        std::sort(Data.begin(), Data.end(), [](const IndexedData<T>& obj1, const IndexedData<T>& obj2) {
            return obj1.value < obj2.value; // Comparison based on `value`
            });

       
           

        Storage.push_back(Data);
        
    }

    template<typename T>
    auto GetVectorDataWithIndex(int Index)
    {
        const auto data = Storage[Index];
        return data;
    }

    template<typename T>
    vector<T> GetRawVectorData(int Index)
    {
        

        auto tmp = new vector<T>();

        const auto& data = Storage[Index];
        auto intPtr = get_if<vector<IndexedData<int>>>(&data);
        if (intPtr == NULL)
        {
            cout << "Data not found. Check your index" << endl;
        }
        else
        {
            for (const auto& val : *intPtr) {
                tmp->push_back(val.value);
            }
        }

        return *tmp;
    }

    template<typename T>
    T Aggregate(int Index, string Function)
    {
        auto tmp = new vector<T>();

        const auto& data = Storage[Index];
        auto intPtr = get_if<vector<IndexedData<T>>>(&data);
        if (intPtr == NULL)
        {
            cout << "Data not found. Check your index" << endl;
            return 0;
        }
        else
        {
            T Val = 0;
            int Len = 0;
            for (const auto& val : *intPtr) {
                Val+=val.value;
                ++Len;
            }
            if (Function == "sum")
                return Val;
            else if (Function == "avg")
                return Val / Len;
        }
    }


private:
    vector<variant<vector<IndexedData<int>>, vector<IndexedData<double>>, vector<IndexedData<string>>>> Storage;
    
    template<typename T>
    void ReIndexData(int Index)
    {
        auto tmp = new vector<T>();

        const auto& data = Storage[Index];
        auto intPtr = get_if<vector<IndexedData<int>>>(&data);
        if (intPtr == NULL)
        {
            cout << "Data not found. Check your index" << endl;
        }
        else
        {
            for (const auto& val : *intPtr) 
            {
                tmp->push_back(val.value);
            }
        }
    }

};

int main() {
    
    vector<InMemoryTable*>* Data = new vector<InMemoryTable*>();

    //Create A In Memory Data Table
    InMemoryTable* strmem = new InMemoryTable();
    Data->push_back(strmem);
    auto vecstr = new vector<IndexedData<string>>{ { 1,"Mehtap"},{2,"Deniz"},{3,"Ziya"}, {4,"Eþe"},{5,"Abuzer"}};
    strmem->LoadVectorData(*vecstr);

    InMemoryTable* mem = new InMemoryTable();
    Data->push_back(mem);
    auto vecint = new vector<IndexedData<int>>{ { 1,3},{ 2,2 },{ 3,4 }, { 4,8 },{5,6} };
    mem->LoadVectorData(*vecint);

    auto vecdbl = new vector<IndexedData<double>>{ { 1,1.5},{ 2,2.4 },{ 3,3.4444 }, { 4,4.9999 },{5,5.22222} };
    mem->LoadVectorData(*vecdbl);

    auto vec1 = mem->GetVectorDataWithIndex<int>(0);

    vector<int> tst = mem->GetRawVectorData<int>(0);

    for (size_t i = 0; i < tst.size(); i++)
        cout << tst[i] << " ";

    cout << "Sum: " << (*Data)[0]->Aggregate<int>(0, "sum") << endl;
    cout << "Avg: " << (*Data)[0]->Aggregate<int>(0, "avg") << endl;

    cout << "Sum: " << mem->Aggregate<double>(1, "sum") << endl;
    cout << "Avg: " << mem->Aggregate<double>(1, "avg") << endl;

    delete vecint,vecdbl,vecstr;
    delete mem,strmem;
    //vector<variant<vector<IndexedData<int>>, vector<IndexedData<double>>, vector<IndexedData<string>>>> Storage;
    //vector<IndexedData<double>>* x=new vector<IndexedData<double>>();
    //x->push_back({ 1,5.8787 });
    //Storage.push_back(*x);

    //auto vec = new vector<IndexedData<int>>{ { 1,1},{ 2,2 },{ 3,3 }, { 4,4 },{5,5} };
    //vec->push_back({ 6,6 });
    //Storage.push_back(*vec);

    //auto y = new vector<IndexedData<string>>();
    //y->push_back({ 1,"Aykut" });
    //y->push_back({ 2,"Guven" });
    //Storage.push_back(*y);

   
    //int index = 1;
    //const auto& data = Storage[index];
    //auto intPtr = get_if<vector<IndexedData<int>>>(&data);
    //if (intPtr == NULL)
    //{
    //    cout << "Data not found. Check your index" << endl;
    //}
    //else
    //{
    //    cout << "Data found." << endl;
    //    for (const auto& val : *intPtr) {
    //        cout << val.index << " - " << val.value << endl;
    //    }
    //}
    
    
    return 0;
}

