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
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <chrono>
#include <ctime>



using namespace std;
//using DynamicData = variant<IndexedData<int>, IndexedData<double>, IndexedData<std::string>>;

//Data Item to store Data with Index of the Data
template<typename T>
class IndexedData {
public:
    IndexedData<T>(int index,T value) 
    {
        this->index = index;
        this->value = value;
    }
    IndexedData()
    {
       
    }
    int index;  // Row index
    T value;    // Data value

};

class InMemoryTable
{
public:
    InMemoryTable() {}
    string TableName="";
    template<typename T>
    void LoadVectorData(vector<IndexedData<T>> Data)
    {
        //Sort Ascending Order
        std::sort(Data.begin(), Data.end(), [](const IndexedData<T>& obj1, const IndexedData<T>& obj2) {
            return obj1.value < obj2.value; // Comparison based on `value`
            });
        Storage.push_back(Data);        
    }
    vector<string> Split(const string& str, char delimiter) {
        vector<string> tokens;
        stringstream ss(str);
        string token;
        while (getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }
    
    //template<typename T>
    void LoadCSVData(string filepath, bool HasHeader)
    {
        ifstream file(filepath);
        if (!file.is_open()) {
            cerr << "Failed to open file!" << endl;
        }

        string line;
        int row_index = 0;     // Variable to track the row index
       
        // Read the file line by line
        while (getline(file, line))
        {
            //Check If Comment Line or Empty line
            //if (line._Starts_with("#") || line._Starts_with(" / ") || line._Starts_with(" - ") || line._Starts_with(""))
            if (line[0]=='#' || line[0] == '-' || line[0] == '/'|| line.empty())
                continue;
            if (HasHeader) {
                HasHeader = false;
                vector<string> columns_data = Split(line, ',');
                int index = 0;
                for (size_t i = 0; i < columns_data.size(); i++)
                {
                    //AG
                    //Implement IndexedData Object into vector for each type of <T>
                    map<string, int> myMap;
                    vector<string> column_info = Split(columns_data[i], ':');
                    myMap.insert(make_pair(column_info[0], index));
                    IndexDataTypes.insert(make_pair(index, column_info[1]));
                    ColumnsIndex.push_back(myMap);
                    IndexedData tmp = {index,column_info[0]};
                    Columns.push_back(tmp);
                    ++index;
                }
                continue; // Skip the header line
            }
            //Line to Array by split
            vector<string> columns_data = Split(line, ',');

            IndexedData<string>* Str_Data_Pointer = NULL;//new IndexedData<string>();
            IndexedData<string>* Date_Data_Pointer = NULL; //new IndexedData<string>();
            IndexedData<int>* Int_Data_Pointer = NULL;//new IndexedData<int>();
            IndexedData<double>* Dbl_Data_Pointer = NULL;//new IndexedData<double>();

            // Add each column value to the respective column index with its row index
            for (size_t i = 0; i < columns_data.size(); ++i) {
               
                                
                if (IndexDataTypes[i] == "string")
                {
                    delete  Str_Data_Pointer;
                    Str_Data_Pointer = new IndexedData<string>(row_index, columns_data[i]);
                }
                else if (IndexDataTypes[i] == "number" || IndexDataTypes[i] == "integer" || IndexDataTypes[i] == "int")
                {
                    delete   Int_Data_Pointer;
                    Int_Data_Pointer = new IndexedData<int>(row_index, stoi(string(columns_data[i])));
                }
                else if (IndexDataTypes[i] == "bignumber" || IndexDataTypes[i] == "double" || IndexDataTypes[i] == "float")
                {
                    delete  Dbl_Data_Pointer;
                    Dbl_Data_Pointer = new IndexedData<double>(row_index, stod(string(columns_data[i])));
                }
                else if (IndexDataTypes[i] == "date")
                {
                    delete  Date_Data_Pointer;
                    Date_Data_Pointer = new IndexedData<string>(row_index, string(columns_data[i]));
                }//any_cast<std::chrono::system_clock::time_point>(columns_data[i])
                else
                {
                    delete  Str_Data_Pointer;
                    Str_Data_Pointer = new IndexedData<string>(row_index, string(columns_data[i]));
                }

                

                if (Storage.size() < Columns.size())
                {
                    //A new Columnar Vector will be added to Storage
                    if (Str_Data_Pointer != NULL)
                    {
                        vector<IndexedData<string>>* tmp=new vector<IndexedData<string>>();
                        tmp->push_back(*Str_Data_Pointer);
                        Storage.push_back(*tmp);
                        Str_Data_Pointer = NULL;
                        continue;
                    }
                    else if (Int_Data_Pointer != NULL)
                    {
                        vector<IndexedData<int>>* tmp = new vector<IndexedData<int>>();
                        tmp->push_back(*Int_Data_Pointer);
                        Storage.push_back(*tmp);
                        Int_Data_Pointer = NULL;
                        continue;
                    }
                    else if (Dbl_Data_Pointer != NULL)
                    {
                        vector<IndexedData<double>>* tmp = new vector<IndexedData<double>>();
                        tmp->push_back(*Dbl_Data_Pointer);
                        Storage.push_back(*tmp);
                        Dbl_Data_Pointer = NULL;
                        continue;
                    }
                    else if (Date_Data_Pointer != NULL)
                    {
                        vector<IndexedData<string>>* tmp = new vector<IndexedData<string>>();
                        tmp->push_back(*Date_Data_Pointer);
                        Storage.push_back(*tmp);
                        Date_Data_Pointer = NULL;
                        continue;
                    }
                    else
                    {
                        //Default Data Type Keep String
                        vector<IndexedData<string>>* tmp = new vector<IndexedData<string>>();
                        tmp->push_back(*Str_Data_Pointer);
                        Storage.push_back(*tmp);
                        Str_Data_Pointer = NULL;
                        continue;
                    }
                }
                else
                {
                    string colname = Columns[i].value;
                    string coltype=IndexDataTypes[i];
                    if (coltype == "string")
                    {
                        
                        auto& data = Storage[i];
                        auto DataVec = get_if<vector<IndexedData<string>>>(&data);
                        if (DataVec == NULL)
                        {
                            cout << "Data not found. Check your index" << endl;
                        }
                        else
                        {      
                            DataVec->push_back(*Str_Data_Pointer);
                        }
                        continue;
                    }
                    else if (coltype == "int")
                    {
                        auto& data = Storage[i];
                        auto DataVec = get_if<vector<IndexedData<int>>>(&data);
                        if (DataVec == NULL)
                        {
                            cout << "Data not found. Check your index" << endl;
                        }
                        else
                        {
                            DataVec->push_back(*Int_Data_Pointer);
                        }
                        continue;
                    }
                    else if(coltype=="double")
                    {
                        auto& data = Storage[i];
                        auto DataVec = get_if<vector<IndexedData<double>>>(&data);
                        if (DataVec == NULL)
                        {
                            cout << "Data not found. Check your index" << endl;
                        }
                        else
                        {
                            DataVec->push_back(*Dbl_Data_Pointer);
                        }
                        continue;
                    }
                    else if(coltype=="date")
                    {
                        auto& data = Storage[i];
                        auto DataVec = get_if<vector<IndexedData<string>>>(&data);
                        if (DataVec == NULL)
                        {
                            cout << "Data not found. Check your index" << endl;
                        }
                        else
                        {
                            DataVec->push_back(*Date_Data_Pointer);
                        }
                        continue;
                    }
                    else
                    {
                        auto& data = Storage[i];
                        auto DataVec = get_if<vector<IndexedData<string>>>(&data);
                        if (DataVec == NULL)
                        {
                            cout << "Data not found. Check your index" << endl;
                        }
                        else
                        {
                            DataVec->push_back(*Str_Data_Pointer);
                        }
                    }
                }

            }

            // Increment row index
            row_index++;

        }
        file.close();
        ReIndexData();
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
    vector<map<string, int>> ColumnsIndex;
    vector<IndexedData<string>> Columns;
    map<int, string> IndexDataTypes;
   
    vector<variant<vector<IndexedData<int>>, vector<IndexedData<double>>, vector<IndexedData<string>>>> Storage;
    void ReIndexData(int Index=-1)
    {
        int LowerBound = 0;
        int UpperBound = Storage.size();

        if (Index != -1)
        {
            LowerBound = Index;
            UpperBound = Index + 1;
        }

        for (size_t i = LowerBound; i < UpperBound; i++)
        {


            if (IndexDataTypes[i] == "string")
            {
                auto& data = Storage[i];
                auto Data = get_if<vector<IndexedData<string>>>(&data);
                //Sort Ascending Order
                std::sort((*Data).begin(), (*Data).end(), [](const IndexedData<string>& obj1, const IndexedData<string>& obj2) {
                    return obj1.value < obj2.value; // Comparison based on `value`
                    });
            }
            else if (IndexDataTypes[i] == "number" || IndexDataTypes[i] == "integer" || IndexDataTypes[i] == "int")
            {
                auto& data = Storage[i];
                auto Data = get_if<vector<IndexedData<int>>>(&data);
                //Sort Ascending Order
                std::sort((*Data).begin(), (*Data).end(), [](const IndexedData<int>& obj1, const IndexedData<int>& obj2) {
                    return obj1.value < obj2.value; // Comparison based on `value`
                    });
            }
            else if (IndexDataTypes[i] == "bignumber" || IndexDataTypes[i] == "double" || IndexDataTypes[i] == "float")
            {
                auto& data = Storage[i];
                auto Data = get_if<vector<IndexedData<double>>>(&data);
                //Sort Ascending Order
                std::sort((*Data).begin(), (*Data).end(), [](const IndexedData<double>& obj1, const IndexedData<double>& obj2) {
                    return obj1.value < obj2.value; // Comparison based on `value`
                    });
            }
            else if (IndexDataTypes[i] == "date")
            {
                auto& data = Storage[i];
                auto Data = get_if<vector<IndexedData<string>>>(&data);
                //Sort Ascending Order
                std::sort((*Data).begin(), (*Data).end(), [](const IndexedData<string>& obj1, const IndexedData<string>& obj2) {
                    return obj1.value < obj2.value; // Comparison based on `value`
                    });
            }//any_cast<std::chrono::system_clock::time_point>(columns_data[i])
            else
            {
                auto& data = Storage[i];
                auto Data = get_if<vector<IndexedData<string>>>(&data);
                //Sort Ascending Order
                std::sort((*Data).begin(), (*Data).end(), [](const IndexedData<string>& obj1, const IndexedData<string>& obj2) {
                    return obj1.value < obj2.value; // Comparison based on `value`
                    });
            }

        }
        
      
    }

};
string ShowMenu();
vector<InMemoryTable*>* Data = new vector<InMemoryTable*>();
int main() {
    //Driver Code to test functionalities
    //Vector to store All Data
    
    
    ////Data 1 : Create Data from CSV File
    //InMemoryTable* filemem = new InMemoryTable();
    //filemem->LoadCSVData("data.csv", true);
    //
    ////Data 2 : Create Data from Vector String
    //InMemoryTable* strmem = new InMemoryTable();
    //Data->push_back(strmem);
    //auto vecstr = new vector<IndexedData<string>>{ { 1,"Mehtap"},{2,"Deniz"},{3,"Ziya"}, {4,"Eþe"},{5,"Abuzer"}};
    //strmem->LoadVectorData(*vecstr);

    ////Data 2 : Create Data from Vector integer
    //InMemoryTable* mem = new InMemoryTable();
    //Data->push_back(mem);
    //auto vecint = new vector<IndexedData<int>>{ { 1,3},{ 2,2 },{ 3,4 }, { 4,8 },{5,6} };
    //mem->LoadVectorData(*vecint);

    ////Data 2 : Create Data from Vector double
    //auto vecdbl = new vector<IndexedData<double>>{ { 1,1.5},{ 2,2.4 },{ 3,3.4444 }, { 4,4.9999 },{5,5.22222} };
    //mem->LoadVectorData(*vecdbl);


    ////Some stuff to test.
    ////------------------------------------------------------------------------------------------------------
    //auto vec1 = mem->GetVectorDataWithIndex<int>(0);

    //vector<int> tst = mem->GetRawVectorData<int>(0);

    //for (size_t i = 0; i < tst.size(); i++)
    //    cout << tst[i] << " ";

    //cout << "Sum: " << (*Data)[0]->Aggregate<int>(0, "sum") << endl;
    //cout << "Avg: " << (*Data)[0]->Aggregate<int>(0, "avg") << endl;

    //cout << "Sum: " << mem->Aggregate<double>(1, "sum") << endl;
    //cout << "Avg: " << mem->Aggregate<double>(1, "avg") << endl;

    //delete vecint,vecdbl,vecstr;
    //delete mem,strmem;
  
    string state = "o";
    while (state != "e" && state != "E")
    {
        state=ShowMenu();
    }
        
    
    
    return 0;
}

string ShowMenu()
{
    cout << "\033[2J\033[H";
    char Choise = 'S';
    cout << "In Memory Data Processing Menu" << endl;
    cout << "-------------------------------" << endl;
    cout << "1- Load Data press (L)" << endl;
    cout << "2- Execute Query   (Q)" << endl;
    cout << "3- Aggregate Query (A)" << endl;
    cout << "4- Exit            (E)" << endl;
    cout << endl;
    cout << "your choice:";
    cin >> Choise;
    switch (Choise)
    {
    case 'L':
    case 'l':
    {
        string dataName = "";
        string filePath = "";
        char Selection = 'N';
        cout << "Enter Dataname:";
        cin >> dataName;
        cout << endl;
        cout << "Enter File Path:";
        cin >> filePath;
        cout << "-------------------------------" << endl;
        cout << "You have entered following." << endl;
        cout << "Dataname:" << dataName << "/File Path:" << filePath << endl;
        cout << "Proceed? Yes(Y) No(N)";
        cin >> Selection;
        if (Selection == 'Y' || Selection == 'y')
        {
            InMemoryTable* filemem = new InMemoryTable();
            filemem->TableName = dataName;
            filemem->LoadCSVData(filePath, true);
            Data->push_back(filemem);
           
        }
        else
            ShowMenu();
        break;
    }
       
    case 'Q':
    case 'q':
    {
        string Query = "";
        char Selection = 'N';
        cout << "-------------------------------" << endl;
        cout << "Enter Query is here" << endl;
        cin >> Query;
        cout << "-------------------------------" << endl;
        cout << "You have entered following." << endl;
        cout << Query << endl;
        cout << "Proceed? Yes(Y) No(N)";
        cin >> Selection;
        if (Selection == 'Y' || Selection == 'y')
        {
            cout << "Query validated" << endl;
            
        }
        else
            ShowMenu();
        break;
    }
       
    case 'A':
    case 'a':
    {
        string Query = "";
        char Selection = 'N';
        cout << "-------------------------------" << endl;
        cout << "Enter Aggregate Query is here" << endl;
        cin >> Query;
        cout << "-------------------------------" << endl;
        cout << "You have entered following." << endl;
        cout << Query << endl;
        cout << "Proceed? Yes(Y) No(N)";
        cin >> Selection;
        if (Selection == 'Y' || Selection == 'y')
        {
            cout << "Query validated" << endl;
        }
        else
            ShowMenu();

        break;
    }
    case 'E':
    case 'e':
        return "e";
    //break;
    default:
        cout << "Invalid selection" << endl;
        ShowMenu();
        break;
    }
    return "o";
}

