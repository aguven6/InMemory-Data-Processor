
# In Memory Data Processing
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
1- Calculate  Sum,Count,Avg, Max, Min for whole data

2- Calculate  Sum,Count,Avg, Max, Min with Filtering

3- Group By a Key and aggregate values for whole data

4- Group By a Key and aggregate values with Filtering

-----------------------------------------------------------------------------------------------------------------
When we complete this task. We will use GPUs to process data. 
NEXT Step will be GPU usage for computing. We will divide and conquer that This is what Computer Science loves.
