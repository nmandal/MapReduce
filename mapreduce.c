#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>

#define MAPS 809                           // Large prime number

void *worker_mapper(void *);
void *worker_reducer(void *);
char *get_next(char *, int);
unsigned long MR_DefaultHashPartition(char *, int);
int comparator(const void *, const void *);

typedef struct __value_t_ value_t;
typedef struct __keys_t_ keys_t;
typedef struct __entry_t_ entry_t;
typedef struct __hashtable_t_ ht_t;

struct __value_t_ {
  char *value;
  struct __value_t_ *next;
};

struct __keys_t_ {
  char *key;
  value_t *head;
  struct __keys_t_ *next;
};

struct __entry_t_ {
  keys_t *head;
  pthread_mutex_t lock;
};

struct __hashtable_t_ {
  entry_t map[MAPS];
  keys_t *sorted;
  int num_keys;
  int visit;
  pthread_mutex_t lock;
};

int NUM_REDUCER;
int NUM_FILES;
char **file_names;
int count;
Mapper mapper;
Reducer reducer;
Partitioner partitioner;
pthread_mutex_t job_lock;
ht_t ht[64];

void initialize(int argc, char *argv[], Mapper mapp, int num_reducers, Partitioner part, Reducer red) {
  int k = pthread_mutex_init(&job_lock, NULL);
  assert(k == 0);

  // Initialize globals
  NUM_FILES    = argc - 1;
  file_names   = argv + 1;
  mapper       = mapp;
  count        = 0;
  NUM_REDUCER  = num_reducers;
  partitioner  = part;
  reducer      = red;

  // Initialize hash tables
  for (int i = 0; i < NUM_REDUCER; i++) {
    pthread_mutex_init(&ht[i].lock, NULL);
    ht[i].num_keys = 0;
    ht[i].visit    = 0;
    ht[i].sorted   = NULL;
    
    for (int j = 0; j < MAPS; j++) {
      ht[i].map[j].head = NULL;
      pthread_mutex_init(&ht[i].map[j].lock, NULL);
    }
  }

  // Sort longest file first linear cuz why not
  struct stat l, c;
  // Ugly linear search cuz why not?
  for(int i = 1; i < argc; i++) {
    int l_i = i;
    stat(argv[i], &l);
    for(int j = i+1; j < argc; j++) {
      stat(argv[j], &c);
      if(c.st_size > l.st_size) {
	stat(argv[j], &l);
	l_i = j;
      }
    }
    char *dup_i = argv[i];
    char *dup_l = argv[l_i];
    argv[i]     = dup_l;
    argv[l_i]   = dup_i;
  }
}

void *worker_mapper(void *args) {
  while (1) {
    char *file;
    pthread_mutex_lock(&job_lock);
    if (count >= NUM_FILES) {
      pthread_mutex_unlock(&job_lock);
      return NULL;
    }
    file = file_names[count++];
    pthread_mutex_unlock(&job_lock);
    (*mapper)(file);
  }
}

void *worker_reducer(void *args) {
  int partition_num = *(int*)args;
  free(args);
  args = NULL;
  if (ht[partition_num].num_keys == 0)
    return NULL;
  ht[partition_num].sorted = malloc(sizeof(keys_t) * ht[partition_num].num_keys);
  
  int counter = 0;
  for (int i = 0; i < MAPS; i++) {
    keys_t *curr = ht[partition_num].map[i].head;
    if (curr == NULL)
      continue;
    while (curr != NULL) {
      ht[partition_num].sorted[counter] = *curr;
      counter++;
      curr = curr->next;
    }
  }
  
  qsort(ht[partition_num].sorted, ht[partition_num].num_keys, sizeof(keys_t), comparator);

  for (int j = 0; j < ht[partition_num].num_keys; j++) {
    char *key = ht[partition_num].sorted[j].key;
    (*reducer)(key, get_next, partition_num);
  }

  for (int k = 0; k < MAPS; k++) {
    keys_t *curr = ht[partition_num].map[k].head;
    if (curr == NULL)
      continue;
    while (curr != NULL) {
      free(curr->key);
      curr->key = NULL;
      value_t *curr_val = curr->head;
      while (curr_val != NULL) {
        free(curr_val->value);
        curr_val->value = NULL;
        value_t *tmp = curr_val->next;
        free(curr_val);
        curr_val = tmp;
      }
      curr_val = NULL;
      keys_t *tmp_key = curr->next;
      free(curr);
      curr = tmp_key;
    }  
    curr = NULL;
  }
  free(ht[partition_num].sorted);
  ht[partition_num].sorted = NULL;

  return NULL;
}

int comparator(const void *a, const void *b) {
  char *a1 = ((keys_t*)a)->key;
  char *b1 = ((keys_t*)b)->key;

  return strcmp(a1, b1);
}

char *get_next(char *key, int partition_number) {
  keys_t *arr = ht[partition_number].sorted;
  char *value;

  while (1) {
    int curr = ht[partition_number].visit;
    if (strcmp(arr[curr].key, key) == 0) {
      if (arr[curr].head == NULL) 
        return NULL;
      value_t *tmp = arr[curr].head->next;
      value = arr[curr].head->value;
      arr[curr].head = tmp;
      return value;  
    } else {
      ht[partition_number].visit++;
      continue;
    }
    return NULL;
  }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition) {
  
  initialize(argc, argv, map, num_reducers, partition, reduce);

  // create mapper threads
  pthread_t threadsM[num_mappers];
  for (int i = 0; i < num_mappers; i++) {
    pthread_create(&threadsM[i], NULL, worker_mapper, NULL);
  }

  // wait for threads to finish and join
  for (int j = 0; j < num_mappers; j++) {
    pthread_join(threadsM[j], NULL);
  }

  // create reducer threads
  pthread_t threadsR[num_reducers];
  for (int k = 0; k < num_reducers; k++) {
    void *args = malloc(4);
    *(int*)args = k;
    pthread_create(&threadsR[k], NULL, worker_reducer, args);
  }

  for (int l = 0; l < num_reducers; l++) {
    pthread_join(threadsR[l], NULL);
  }
}

void MR_Emit(char *key, char *value) {
  unsigned long partition_num = (*partitioner)(key, NUM_REDUCER);
  unsigned long map_num = MR_DefaultHashPartition(key, MAPS);
  pthread_mutex_lock(&ht[partition_num].map[map_num].lock);
  keys_t *tmp = ht[partition_num].map[map_num].head;
  while (tmp != NULL) {
    if (strcmp(tmp->key, key) == 0) 
      break;
    tmp = tmp->next;
  }    
  
  value_t *new_val = malloc(sizeof(value_t));
  if (new_val == NULL) {
    pthread_mutex_unlock(&ht[partition_num].map[map_num].lock);
    return;
  }
  new_val->value = malloc(sizeof(char) * 20);
  strcpy(new_val->value, value);
  new_val->next = NULL;
  
  if (tmp == NULL) {
    keys_t *new_key = malloc(sizeof(keys_t));
    if (new_key == NULL) {
      pthread_mutex_unlock(&ht[partition_num].map[map_num].lock);
      return;
    }
    new_key->head = new_val;
    new_key->next = ht[partition_num].map[map_num].head;
    ht[partition_num].map[map_num].head = new_key;

    new_key->key = malloc(sizeof(char) * 20);
    strcpy(new_key->key, key);
    pthread_mutex_lock(&ht[partition_num].lock);
    ht[partition_num].num_keys++;
    pthread_mutex_unlock(&ht[partition_num].lock);
  } else {
    new_val->next = tmp->head;
    tmp->head = new_val;
  }

  pthread_mutex_unlock(&ht[partition_num].map[map_num].lock);
}

