# data-science-projects
space to learn and try out new projects


Word_Count.py 
Counting a 5-word Sequence:
I started the assignment by modifying the word_count.py code given in class. I used Python and Spark for the assignment. I read the input text file. Found the length of the RDD and created an empty list. Using a For Loop, I ran through the RDD and added 5 words in a list that was created. Then I tried to pass on that list for Map and Reduce function but it wouldn’t move ahead to give me an output

Matrix_Multiplication:


1) The code in this file is for Matrix Multiplication of two matrices using Map Reduce. In this code, we first initialise the matrix as given in the question

2) Then, we created a function create_values. As for matrix multiplication, we have to pass keys and values. The values are in the format (i,j,Mij) for Matrix 1. i is the value of the row, j is the column and Mij the value in cell i,j. The same way, for Matrix 2, we pass the values in the format (j,k,Njk), with j the row number, k the column number and Njk the value in the cell j,k.

3) We apply Map function to both these values. for value_1, the map function generates the key value pairs in the format:
(i,k):(i,j,Mij).

4) For value_2, the Map function generates the key value pairs in the format: (i,k):(j,k,Njk)

5) Then value_1 and value_2 in the key:value format is passed. For the keys (i,k), where the j is equal, the values Mij and Njk are multiplied. The multiplied values are stored in the the variable kv_pairs. kv_pairs is then converted to pipelineRDD.

6)This kv_pairs is then sent to the ReducebyKey function, where for all the values of the same key (i,k), we add the MijxNjk values to get our final matrix.
