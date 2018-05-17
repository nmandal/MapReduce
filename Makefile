mapreduce: mapreduce.c
	gcc -c -o mapreduce.o mapreduce.c -Wall -Werror -pthread -O
	gcc -o wordcount wordcount.c mapreduce.o -Wall -Werror -pthread
