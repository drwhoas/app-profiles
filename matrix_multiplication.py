#In this code, we input two matrices and multiply them using Map-Reduce

from pyspark import SparkContext
sc = SparkContext(appName='SparkMatrixMultiplication')

#initializing the matrix 
matrix_1,matrix_2 = [[6,2,4],[1,9,2],[4,6,6]],[[6,3,1],[7,1,2],[2,8,1]]

#in the function create values, we're creating a variable, which has the following values: i,j, Mij. This variable will be the value when the map function takes key:value pairs. 
def create_values(matrix):
  list_1 = []
  num1=0
  while num1 < len(matrix):
    num2=0
    while num2 < len(matrix[1]):
      list_1.append((num1,num2,matrix[num1][num2]))
      num2+=1
    num1+=1
  return sc.parallelize(list_1)

#we pass the the matrices to our create_values function to get two variables value 1 and value 2
value_1 = create_values(matrix_1)
value_2 = create_values(matrix_2)

#applying the map function and generating key value pairs (i,j,Mij) for matrix_1 and (j,k,Njk) for matrix_2.
value_1 = value_1.map(lambda (x,y,z) : (x,(y,z))).collect()
value_2 = value_2.map(lambda (d,e,f) : (e,(d,f))).collect()

#here, we pass our generated key value pairs from Map function. value_1 consists of (i,j,Mij) and value_2 consists of (j,k,Njk). Then for all the matrices with the key (i,k), if the value of j is the same, we multiply the values of Mij to Njk.  
keyvalue_pairs=[]
num1=0
while num1 < len(value_1):
   num2=0
   while num2 < len(value_2):
      if value_1[num1][1][0] != value_2[num2][1][0]:
         pass
      else:
	 keyvalue_pairs.append(((value_1[num1][0],value_2[num2][0]),(value_1[num1][1][1]*value_2[num2][1][1])))
      num2+=1
   num1+=1

#converting kv_pairs to a pipeline RDD
keyvalue_pairs = sc.parallelize(keyvalue_pairs)

#In the Reduce function, all the MijxNjk values for the key (i,k) is added. This gives us the values of the final matrix. 
result = keyvalue_pairs.reduceByKey(lambda x,y: x+y )
print("The final matrix is ",result.collect())

