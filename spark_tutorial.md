
# RDD creation

#### [Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

In this notebook we will introduce two different ways of getting data into the basic Spark data structure, the **Resilient Distributed Dataset** or **RDD**. An RDD is a distributed collection of elements. All work in Spark is expressed as either creating new RDDs, transforming existing RDDs, or calling actions on RDDs to compute a result. Spark automatically distributes the data contained in RDDs across your cluster and parallelizes the operations you perform on them.

#### References

The reference book for these and other Spark related topics is *Learning Spark* by Holden Karau, Andy Konwinski, Patrick Wendell, and Matei Zaharia.  

The KDD Cup 1999 competition dataset is described in detail [here](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99).

## Getting the data files  

In this notebook we will use the reduced dataset (10 percent) provided for the KDD Cup 1999, containing nearly half million network interactions. The file is provided as a *Gzip* file that we will download locally.  


```{python}
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```

## Creating a RDD from a file  

The most common way of creating an RDD is to load it from a file. Notice that Spark's `textFile` can handle compressed files directly.    


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
```

Now we have our data file loaded into the `raw_data` RDD.

Without getting into Spark *transformations* and *actions*, the most basic thing we can do to check that we got our RDD contents right is to `count()` the number of lines loaded from the file into the RDD.  


```
raw_data.count()
```
    494021

We can also check the first few entries in our data.  


```
raw_data.take(5)
```

    [u'0,tcp,http,SF,181,5450,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.00,0.00,0.00,0.00,1.00,0.00,0.00,9,9,1.00,0.00,0.11,0.00,0.00,0.00,0.00,0.00,normal.',
     u'0,tcp,http,SF,239,486,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.00,0.00,0.00,0.00,1.00,0.00,0.00,19,19,1.00,0.00,0.05,0.00,0.00,0.00,0.00,0.00,normal.',
     u'0,tcp,http,SF,235,1337,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,8,8,0.00,0.00,0.00,0.00,1.00,0.00,0.00,29,29,1.00,0.00,0.03,0.00,0.00,0.00,0.00,0.00,normal.',
     u'0,tcp,http,SF,219,1337,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,6,6,0.00,0.00,0.00,0.00,1.00,0.00,0.00,39,39,1.00,0.00,0.03,0.00,0.00,0.00,0.00,0.00,normal.',
     u'0,tcp,http,SF,217,2032,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,6,6,0.00,0.00,0.00,0.00,1.00,0.00,0.00,49,49,1.00,0.00,0.02,0.00,0.00,0.00,0.00,0.00,normal.']



In the following notebooks, we will use this raw data to learn about the different Spark transformations and actions.  

## Creating and RDD using `parallelize`

Another way of creating an RDD is to parallelize an already existing list.  


```
a = range(100)

data = sc.parallelize(a)
```

As we did before, we can `count()` the number of elements in the RDD.


```
data.count()
```




    100



As before, we can access the first few elements on our RDD.  


```
data.take(5)
```




    [0, 1, 2, 3, 4]



# RDD basics

#### [Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

This notebook will introduce three basic but essential Spark operations. Two of them are the *transformations* `map` and `filter`. The other is the *action* `collect`. At the same time we will introduce the concept of *persistence* in Spark.    

## Getting the data and creating the RDD

As we did in our first notebook, we will use the reduced dataset (10 percent) provided for the KDD Cup 1999, containing nearly half million network interactions. The file is provided as a Gzip file that we will download locally.


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```

Now we can use this file to create our RDD.


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
```

## The `filter` transformation

This transformation can be applied to RDDs in order to keep just elements that satisfy a certain condition. More concretely, a function is evaluated on every element in the original RDD. The new resulting RDD will contain just those elements that make the function return `True`.

For example, imagine we want to count how many `normal.` interactions we have in our dataset. We can filter our `raw_data` RDD as follows.  


```
normal_raw_data = raw_data.filter(lambda x: 'normal.' in x)
```

Now we can count how many elements we have in the new RDD.


```
from time import time
t0 = time()
normal_count = normal_raw_data.count()
tt = time() - t0
print "There are {} 'normal' interactions".format(normal_count)
print "Count completed in {} seconds".format(round(tt,3))
```

    There are 97278 'normal' interactions
    Count completed in 5.951 seconds


Remember from notebook 1 that we have a total of 494021 in our 10 percent dataset. Here we can see that 97278 contain the `normal.` tag word.  

Notice that we have measured the elapsed time for counting the elements in the RDD. We have done this because we wanted to point out that actual (distributed) computations in Spark take place when we execute *actions* and not *transformations*. In this case `count` is the action we execute on the RDD. We can apply as many transformations as we want on a our RDD and no computation will take place until we call the first action that, in this case takes a few seconds to complete.

## The `map` transformation

By using the `map` transformation in Spark, we can apply a function to every element in our RDD. Python's lambdas are specially expressive for this particular.

In this case we want to read our data file as a CSV formatted one. We can do this by applying a lambda function to each element in the RDD as follows.


```
from pprint import pprint
csv_data = raw_data.map(lambda x: x.split(","))
t0 = time()
head_rows = csv_data.take(5)
tt = time() - t0
print "Parse completed in {} seconds".format(round(tt,3))
pprint(head_rows[0])
```

    Parse completed in 1.715 seconds
    [u'0',
     u'tcp',
     u'http',
     u'SF',
     u'181',
     u'5450',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'1',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'0',
     u'8',
     u'8',
     u'0.00',
     u'0.00',
     u'0.00',
     u'0.00',
     u'1.00',
     u'0.00',
     u'0.00',
     u'9',
     u'9',
     u'1.00',
     u'0.00',
     u'0.11',
     u'0.00',
     u'0.00',
     u'0.00',
     u'0.00',
     u'0.00',
     u'normal.']


Again, all action happens once we call the first Spark *action* (i.e. *take* in this case). What if we take a lot of elements instead of just the first few?  


```
t0 = time()
head_rows = csv_data.take(100000)
tt = time() - t0
print "Parse completed in {} seconds".format(round(tt,3))
```

    Parse completed in 8.629 seconds


We can see that it takes longer. The `map` function is applied now in a  distributed way to a lot of elements on the RDD, hence the longer execution time.

### Using `map` and predefined functions

Of course we can use predefined functions with `map`. Imagine we want to have each element in the RDD as a key-value pair where the key is the tag (e.g. *normal*) and the value is the whole list of elements that represents the row in the CSV formatted file. We could proceed as follows.    


```
def parse_interaction(line):
    elems = line.split(",")
    tag = elems[41]
    return (tag, elems)

key_csv_data = raw_data.map(parse_interaction)
head_rows = key_csv_data.take(5)
pprint(head_rows[0])
```

    (u'normal.',
     [u'0',
      u'tcp',
      u'http',
      u'SF',
      u'181',
      u'5450',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'1',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'0',
      u'8',
      u'8',
      u'0.00',
      u'0.00',
      u'0.00',
      u'0.00',
      u'1.00',
      u'0.00',
      u'0.00',
      u'9',
      u'9',
      u'1.00',
      u'0.00',
      u'0.11',
      u'0.00',
      u'0.00',
      u'0.00',
      u'0.00',
      u'0.00',
      u'normal.'])


That was easy, wasn't it?

In our notebook about working with key-value pairs we will use this type of RDDs to do data aggregations (e.g. count by key).

## The `collect` action

So far we have used the actions `count` and `take`. Another basic action we need to learn is `collect`. Basically it will get all the elements in the RDD into memory for us to work with them. For this reason it has to be used with care, specially when working with large RDDs.  

An example using our raw data.    


```
t0 = time()
all_raw_data = raw_data.collect()
tt = time() - t0
print "Data collected in {} seconds".format(round(tt,3))
```

    Data collected in 17.927 seconds


That took longer as any other action we used before, of course. Every Spark worker node that has a fragment of the RDD has to be coordinated in order to retrieve its part, and then *reduce* everything together.    

As a last example combining all the previous, we want to collect all the `normal` interactions as key-value pairs.   


```
# get data from file
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)

# parse into key-value pairs
key_csv_data = raw_data.map(parse_interaction)

# filter normal key interactions
normal_key_interactions = key_csv_data.filter(lambda x: x[0] == "normal.")

# collect all
t0 = time()
all_normal = normal_key_interactions.collect()
tt = time() - t0
normal_count = len(all_normal)
print "Data collected in {} seconds".format(round(tt,3))
print "There are {} 'normal' interactions".format(normal_count)
```

    Data collected in 12.485 seconds
    There are 97278 normal interactions


This count matches with the previous count for `normal` interactions. The new procedure is more time consuming. This is because we retrieve all the data with `collect` and then use Python's `len` on the resulting list. Before we were just counting the total number of elements in the RDD by using `count`.  

# Sampling RDDs

[Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

So far we have introduced RDD creation together with some basic transformations such as `map` and `filter` and some actions such as `count`, `take`, and `collect`.  

This notebook will show how to sample RDDs. Regarding transformations, `sample` will be introduced since it will be useful in many statistical learning scenarios. Then we will compare results with the `takeSample` action.      

## Getting the data and creating the RDD

In this case we will use the complete dataset provided for the KDD Cup 1999, containing nearly half million network interactions. The file is provided as a Gzip file that we will download locally.


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz", "kddcup.data.gz")
```

Now we can use this file to create our RDD.


```
data_file = "./kddcup.data.gz"
raw_data = sc.textFile(data_file)
```

## Sampling RDDs   

In Spark, there are two sampling operations, the transformation `sample` and the action `takeSample`. By using a transformation we can tell Spark to apply successive transformation on a sample of a given RDD. By using an action we retrieve a given sample and we can have it in local memory to be used by any other standard library (e.g. Scikit-learn).  

### The `sample` transformation

The `sample` transformation takes up to three parameters. First is whether the sampling is done with replacement or not. Second is the sample size as a fraction. Finally we can optionally provide a *random seed*.  


```
raw_data_sample = raw_data.sample(False, 0.1, 1234)
sample_size = raw_data_sample.count()
total_size = raw_data.count()
print "Sample size is {} of {}".format(sample_size, total_size)
```

    Sample size is 489957 of 4898431


But the power of sampling as a transformation comes from doing it as part of a sequence of additional transformations. This will show more powerful once we start doing aggregations and key-value pairs operations, and will be specially useful when using Spark's machine learning library MLlib.    

In the meantime, imagine we want to have an approximation of the proportion of `normal.` interactions in our dataset. We could do this by counting the total number of tags as we did in previous notebooks. However we want a quicker response and we don't need the exact answer but just an approximation. We can do it as follows.   


```
from time import time

# transformations to be applied
raw_data_sample_items = raw_data_sample.map(lambda x: x.split(","))
sample_normal_tags = raw_data_sample_items.filter(lambda x: "normal." in x)

# actions + time
t0 = time()
sample_normal_tags_count = sample_normal_tags.count()
tt = time() - t0

sample_normal_ratio = sample_normal_tags_count / float(sample_size)
print "The ratio of 'normal' interactions is {}".format(round(sample_normal_ratio,3)) 
print "Count done in {} seconds".format(round(tt,3))
```

    The ratio of 'normal' interactions is 0.199
    Count done in 44.523 seconds


Let's compare this with calculating the ratio without sampling.  


```
# transformations to be applied
raw_data_items = raw_data.map(lambda x: x.split(","))
normal_tags = raw_data_items.filter(lambda x: "normal." in x)

# actions + time
t0 = time()
normal_tags_count = normal_tags.count()
tt = time() - t0

normal_ratio = normal_tags_count / float(total_size)
print "The ratio of 'normal' interactions is {}".format(round(normal_ratio,3)) 
print "Count done in {} seconds".format(round(tt,3))
```

    The ratio of 'normal' interactions is 0.199
    Count done in 91.09 seconds


We can see a gain in time. The more transformations we apply after the sampling the bigger this gain. This is because without sampling all the transformations are applied to the complete set of data.  

### The `takeSample` action  

If what we need is to grab a sample of raw data from our RDD into local memory in order to be used by other non-Spark libraries, `takeSample` can be used.  

The syntax is very similar, but in this case we specify the number of items instead of the sample size as a fraction of the complete data size.  


```
t0 = time()
raw_data_sample = raw_data.takeSample(False, 400000, 1234)
normal_data_sample = [x.split(",") for x in raw_data_sample if "normal." in x]
tt = time() - t0

normal_sample_size = len(normal_data_sample)

normal_ratio = normal_sample_size / 400000.0
print "The ratio of 'normal' interactions is {}".format(normal_ratio)
print "Count done in {} seconds".format(round(tt,3))
```

    The ratio of 'normal' interactions is 0.1988025
    Count done in 76.166 seconds


The process was very similar as before. We obtained a sample of about 10 percent of the data, and then filter and split.  

However, it took longer, even with a slightly smaller sample. The reason is that Spark just distributed the execution of the sampling process. The filtering and splitting of the results were done locally in a single node.  

# Set operations on RDDs

[Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

Spark supports many of the operations we have in mathematical sets, such as union and intersection, even when the RDDs themselves are not properly sets. It is important to note that these operations require that the RDDs being operated on are of the same type.  

Set operations are quite straightforward to understand as it work as expected. The only consideration comes from the fact that RDDs are not real sets, and therefore operations such as the union of RDDs doesn't remove duplicates. In this notebook we will have a brief look at `subtract`, `distinct`, and `cartesian`.       

## Getting the data and creating the RDD

As we did in our first notebook, we will use the reduced dataset (10 percent) provided for the KDD Cup 1999, containing nearly half million network interactions. The file is provided as a Gzip file that we will download locally.


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
```

## Getting attack interactions using `subtract`

For illustrative purposes, imagine we already have our RDD with non attack (normal) interactions from some previous analysis.   


```
normal_raw_data = raw_data.filter(lambda x: "normal." in x)
```

We can obtain attack interactions by subtracting normal ones from the original unfiltered RDD as follows.  


```
attack_raw_data = raw_data.subtract(normal_raw_data)
```

Let's do some counts to check our results.  


```
from time import time

# count all
t0 = time()
raw_data_count = raw_data.count()
tt = time() - t0
print "All count in {} secs".format(round(tt,3))
```

    All count in 5.261 secs



```
# count normal
t0 = time()
normal_raw_data_count = normal_raw_data.count()
tt = time() - t0
print "Normal count in {} secs".format(round(tt,3))
```

    Normal count in 5.571 secs



```
# count attacks
t0 = time()
attack_raw_data_count = attack_raw_data.count()
tt = time() - t0
print "Attack count in {} secs".format(round(tt,3))
```

    Attack count in 12.075 secs



```
print "There are {} normal interactions and {} attacks, \
from a total of {} interactions".format(normal_raw_data_count,attack_raw_data_count,raw_data_count)
```

    There are 97278 normal interactions and 396743 attacks, from a total of 494021 interactions


So now we have two RDDs, one with normal interactions and another one with attacks.  

## Protocol and service combinations using `cartesian`

We can compute the Cartesian product between two RDDs by using the `cartesian` transformation. It returns all possible pairs of elements between two RDDs. In our case we will use it to generate all the possible combinations between service and protocol in our network interactions.  

First of all we need to isolate each collection of values in two separate RDDs. For that we will use `distinct` on the CSV-parsed dataset. From the [dataset description](http://kdd.ics.uci.edu/databases/kddcup99/kddcup.names) we know that protocol is the second column and service is the third (tag is the last one and not the first as appears in the page).   

So first, let's get the protocols.  


```
csv_data = raw_data.map(lambda x: x.split(","))
protocols = csv_data.map(lambda x: x[1]).distinct()
protocols.collect()
```




    [u'udp', u'icmp', u'tcp']



Now we do the same for services.  


```
services = csv_data.map(lambda x: x[2]).distinct()
services.collect()
```




    [u'domain',
     u'http_443',
     u'Z39_50',
     u'smtp',
     u'urp_i',
     u'private',
     u'echo',
     u'shell',
     u'red_i',
     u'eco_i',
     u'sunrpc',
     u'ftp_data',
     u'urh_i',
     u'pm_dump',
     u'pop_3',
     u'pop_2',
     u'systat',
     u'ftp',
     u'uucp',
     u'whois',
     u'netbios_dgm',
     u'efs',
     u'remote_job',
     u'daytime',
     u'ntp_u',
     u'finger',
     u'ldap',
     u'netbios_ns',
     u'kshell',
     u'iso_tsap',
     u'ecr_i',
     u'nntp',
     u'printer',
     u'domain_u',
     u'uucp_path',
     u'courier',
     u'exec',
     u'time',
     u'netstat',
     u'telnet',
     u'gopher',
     u'rje',
     u'sql_net',
     u'link',
     u'auth',
     u'netbios_ssn',
     u'csnet_ns',
     u'X11',
     u'IRC',
     u'tftp_u',
     u'login',
     u'supdup',
     u'name',
     u'nnsp',
     u'mtp',
     u'http',
     u'bgp',
     u'ctf',
     u'hostnames',
     u'klogin',
     u'vmnet',
     u'tim_i',
     u'discard',
     u'imap4',
     u'other',
     u'ssh']



A longer list in this case.

Now we can do the cartesian product.  


```
product = protocols.cartesian(services).collect()
print "There are {} combinations of protocol X service".format(len(product))
```

    There are 198 combinations of protocol X service


Obviously, for such small RDDs doesn't really make sense to use Spark cartesian product. We could have perfectly collected the values after using `distinct` and do the cartesian product locally. Moreover, `distinct` and `cartesian` are expensive operations so they must be used with care when the operating datasets are large.    

# Data aggregations on RDDs

[Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

We can aggregate RDD data in Spark by using three different actions: `reduce`, `fold`, and `aggregate`. The last one is the more general one and someway includes the first two.  

## Getting the data and creating the RDD

As we did in our first notebook, we will use the reduced dataset (10 percent) provided for the [KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html), containing nearly half million nework interactions. The file is provided as a Gzip file that we will download locally.  


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
```

## Inspecting interaction duration by tag

Both `fold` and `reduce` take a function as an argument that is applied to two elements of the RDD. The `fold` action differs from `reduce` in that it gets and additional initial *zero value* to be used for the initial call. This value should be the identity element for the function provided.  

As an example, imagine we want to know the total duration of our interactions for normal and attack interactions. We can use `reduce` as follows.    


```
# parse data
csv_data = raw_data.map(lambda x: x.split(","))

# separate into different RDDs
normal_csv_data = csv_data.filter(lambda x: x[41]=="normal.")
attack_csv_data = csv_data.filter(lambda x: x[41]!="normal.")
```

The function that we pass to `reduce` gets and returns elements of the same type of the RDD. If we want to sum durations we need to extract that element into a new RDD.  


```
normal_duration_data = normal_csv_data.map(lambda x: int(x[0]))
attack_duration_data = attack_csv_data.map(lambda x: int(x[0]))
```

Now we can reduce these new RDDs.  


```
total_normal_duration = normal_duration_data.reduce(lambda x, y: x + y)
total_attack_duration = attack_duration_data.reduce(lambda x, y: x + y)

print "Total duration for 'normal' interactions is {}".\
    format(total_normal_duration)
print "Total duration for 'attack' interactions is {}".\
    format(total_attack_duration)
```

    Total duration for 'normal' interactions is 21075991
    Total duration for 'attack' interactions is 2626792


We can go further and use counts to calculate duration means.  


```
normal_count = normal_duration_data.count()
attack_count = attack_duration_data.count()

print "Mean duration for 'normal' interactions is {}".\
    format(round(total_normal_duration/float(normal_count),3))
print "Mean duration for 'attack' interactions is {}".\
    format(round(total_attack_duration/float(attack_count),3))
```

    Mean duration for 'normal' interactions is 216.657
    Mean duration for 'attack' interactions is 6.621


We have a first (and too simplistic) approach to identify attack interactions.

## A better way, using `aggregate`  

The `aggregate` action frees us from the constraint of having the return be the same type as the RDD we are working on. Like with `fold`, we supply an initial zero value of the type we want to return. Then we provide two functions. The first one is used to combine the elements from our RDD with the accumulator. The second function is needed to merge two accumulators. Let's see it in action calculating the mean we did before.  


```
normal_sum_count = normal_duration_data.aggregate(
    (0,0), # the initial value
    (lambda acc, value: (acc[0] + value, acc[1] + 1)), # combine value with acc
    (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])) # combine accumulators
)

print "Mean duration for 'normal' interactions is {}".\
    format(round(normal_sum_count[0]/float(normal_sum_count[1]),3))
```

    Mean duration for 'normal' interactions is 216.657


In the previous aggregation, the accumulator first element keeps the total sum, while the second element keeps the count. Combining an accumulator with an RDD element consists in summing up the value and incrementing the count. Combining two accumulators requires just a pairwise sum.  

We can do the same with attack type interactions.  


```
attack_sum_count = attack_duration_data.aggregate(
    (0,0), # the initial value
    (lambda acc, value: (acc[0] + value, acc[1] + 1)), # combine value with acc
    (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])) # combine accumulators
)

print "Mean duration for 'attack' interactions is {}".\
    format(round(attack_sum_count[0]/float(attack_sum_count[1]),3))
```

    Mean duration for 'attack' interactions is 6.621


# Working with key/value pair RDDs

[Introduction to Spark with Python, by Jose A. Dianes](https://github.com/jadianes/spark-py-notebooks)

Spark provides specific functions to deal with RDDs which elements are key/value pairs. They are usually used to perform aggregations and other processings by key.  

In this notebook we will show how, by working with key/value pairs, we can process our network interactions dataset in a more practical and powerful way than that used in previous notebooks. Key/value pair aggregations will show to be particularly effective when trying to explore each type of tag in our network attacks, in an individual way.  

## Getting the data and creating the RDD

As we did in our first notebook, we will use the reduced dataset (10 percent) provided for the [KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html), containing nearly half million network interactions. The file is provided as a Gzip file that we will download locally.  


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file)
```

## Creating a pair RDD for interaction types

In this notebook we want to do some exploratory data analysis on our network interactions dataset. More concretely we want to profile each network interaction type in terms of some of its variables such as duration. In order to do so, we first need to create the RDD suitable for that, where each interaction is parsed as a CSV row representing the value, and is put together with its corresponding tag as a key.  

Normally we create key/value pair RDDs by applying a function using `map` to the original data. This function returns the corresponding pair for a given RDD element. We can proceed as follows.  


```
csv_data = raw_data.map(lambda x: x.split(","))
key_value_data = csv_data.map(lambda x: (x[41], x)) # x[41] contains the network interaction tag
```

We have now our key/value pair data ready to be used. Let's get the first element in order to see how it looks like.  


```
key_value_data.take(1)
```




    [(u'normal.',
      [u'0',
       u'tcp',
       u'http',
       u'SF',
       u'181',
       u'5450',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'1',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'0',
       u'8',
       u'8',
       u'0.00',
       u'0.00',
       u'0.00',
       u'0.00',
       u'1.00',
       u'0.00',
       u'0.00',
       u'9',
       u'9',
       u'1.00',
       u'0.00',
       u'0.11',
       u'0.00',
       u'0.00',
       u'0.00',
       u'0.00',
       u'0.00',
       u'normal.'])]



## Data aggregations with key/value pair RDDs

We can use all the transformations and actions available for normal RDDs with key/value pair RDDs. We just need to make the functions work with pair elements. Additionally, Spark provides specific functions to work with RDDs containing pair elements. They are very similar to those available for general RDDs.  

For example, we have a `reduceByKey` transformation that we can use as follows to calculate the total duration of each network interaction type.  


```
key_value_duration = csv_data.map(lambda x: (x[41], float(x[0]))) 
durations_by_key = key_value_duration.reduceByKey(lambda x, y: x + y)

durations_by_key.collect()
```




    [(u'guess_passwd.', 144.0),
     (u'nmap.', 0.0),
     (u'warezmaster.', 301.0),
     (u'rootkit.', 1008.0),
     (u'warezclient.', 627563.0),
     (u'smurf.', 0.0),
     (u'pod.', 0.0),
     (u'neptune.', 0.0),
     (u'normal.', 21075991.0),
     (u'spy.', 636.0),
     (u'ftp_write.', 259.0),
     (u'phf.', 18.0),
     (u'portsweep.', 1991911.0),
     (u'teardrop.', 0.0),
     (u'buffer_overflow.', 2751.0),
     (u'land.', 0.0),
     (u'imap.', 72.0),
     (u'loadmodule.', 326.0),
     (u'perl.', 124.0),
     (u'multihop.', 1288.0),
     (u'back.', 284.0),
     (u'ipsweep.', 43.0),
     (u'satan.', 64.0)]



We have a specific counting action for key/value pairs.  


```
counts_by_key = key_value_data.countByKey()
counts_by_key
```




    defaultdict(<type 'int'>, {u'guess_passwd.': 53, u'nmap.': 231, u'warezmaster.': 20, u'rootkit.': 10, u'warezclient.': 1020, u'smurf.': 280790, u'pod.': 264, u'neptune.': 107201, u'normal.': 97278, u'spy.': 2, u'ftp_write.': 8, u'phf.': 4, u'portsweep.': 1040, u'teardrop.': 979, u'buffer_overflow.': 30, u'land.': 21, u'imap.': 12, u'loadmodule.': 9, u'perl.': 3, u'multihop.': 7, u'back.': 2203, u'ipsweep.': 1247, u'satan.': 1589})



### Using `combineByKey`

This is the most general of the per-key aggregation functions. Most of the other per-key combiners are implemented using it. We can think about it as the `aggregate` equivalent since it allows the user to return values that are not the same type as our input data.

For example, we can use it to calculate per-type average durations as follows.  


```
sum_counts = key_value_duration.combineByKey(
    (lambda x: (x, 1)), # the initial value, with value x and count 1
    (lambda acc, value: (acc[0]+value, acc[1]+1)), # how to combine a pair value with the accumulator: sum value, and increment count
    (lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1])) # combine accumulators
)

sum_counts.collectAsMap()
```




    {u'back.': (284.0, 2203),
     u'buffer_overflow.': (2751.0, 30),
     u'ftp_write.': (259.0, 8),
     u'guess_passwd.': (144.0, 53),
     u'imap.': (72.0, 12),
     u'ipsweep.': (43.0, 1247),
     u'land.': (0.0, 21),
     u'loadmodule.': (326.0, 9),
     u'multihop.': (1288.0, 7),
     u'neptune.': (0.0, 107201),
     u'nmap.': (0.0, 231),
     u'normal.': (21075991.0, 97278),
     u'perl.': (124.0, 3),
     u'phf.': (18.0, 4),
     u'pod.': (0.0, 264),
     u'portsweep.': (1991911.0, 1040),
     u'rootkit.': (1008.0, 10),
     u'satan.': (64.0, 1589),
     u'smurf.': (0.0, 280790),
     u'spy.': (636.0, 2),
     u'teardrop.': (0.0, 979),
     u'warezclient.': (627563.0, 1020),
     u'warezmaster.': (301.0, 20)}



We can see that the arguments are pretty similar to those passed to `aggregate` in the previous notebook. The result associated to each type is in the form of a pair. If we want to actually get the averages, we need to do the division before collecting the results.  


```
duration_means_by_type = sum_counts.map(lambda (key,value): (key, round(value[0]/value[1],3))).collectAsMap()

# Print them sorted
for tag in sorted(duration_means_by_type, key=duration_means_by_type.get, reverse=True):
    print tag, duration_means_by_type[tag]
```

    portsweep. 1915.299
    warezclient. 615.258
    spy. 318.0
    normal. 216.657
    multihop. 184.0
    rootkit. 100.8
    buffer_overflow. 91.7
    perl. 41.333
    loadmodule. 36.222
    ftp_write. 32.375
    warezmaster. 15.05
    imap. 6.0
    phf. 4.5
    guess_passwd. 2.717
    back. 0.129
    satan. 0.04
    ipsweep. 0.034
    nmap. 0.0
    smurf. 0.0
    pod. 0.0
    neptune. 0.0
    teardrop. 0.0
    land. 0.0


A small step into understanding what makes a network interaction be considered an attack.

# Spark SQL and Data Frames

[Introduction to Spark with Python, by Jose A. Dianes](http://jadianes.github.io/spark-py-notebooks)

This notebook will introduce Spark capabilities to deal with data in a structured way. Basically, everything turns around the concept of *Data Frame* and using *SQL language* to query them. We will see how the data frame abstraction, very popular in other data analytics ecosystems (e.g. R and Python/Pandas), it is very powerful when performing exploratory data analysis. In fact, it is very easy to express data queries when used together with the SQL language. Moreover, Spark distributes this column-based data structure transparently, in order to make the querying process as efficient as possible.      

## Getting the data and creating the RDD

As we did in previous notebooks, we will use the reduced dataset (10 percent) provided for the [KDD Cup 1999](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html), containing nearly half million nework interactions. The file is provided as a Gzip file that we will download locally.  


```
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")
```


```
data_file = "./kddcup.data_10_percent.gz"
raw_data = sc.textFile(data_file).cache()
```

## Getting a Data Frame

A Spark `DataFrame` is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R or Pandas. They can be constructed from a wide array of sources such as a existing RDD in our case.

The entry point into all SQL functionality in Spark is the `SQLContext` class. To create a basic instance, all we need is a `SparkContext` reference. Since we are running Spark in shell mode (using pySpark) we can use the global context object `sc` for this purpose.    


```
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
```

### Inferring the schema

With a `SQLContext`, we are ready to create a `DataFrame` from our existing RDD. But first we need to tell Spark SQL the schema in our data.   

Spark SQL can convert an RDD of `Row` objects to a `DataFrame`. Rows are constructed by passing a list of key/value pairs as *kwargs* to the `Row` class. The keys define the column names, and the types are inferred by looking at the first row. Therefore, it is important that there is no missing data in the first row of the RDD in order to properly infer the schema.

In our case, we first need to split the comma separated data, and then use the information in KDD's 1999 task description to obtain the [column names](http://kdd.ics.uci.edu/databases/kddcup99/kddcup.names).  


```
from pyspark.sql import Row

csv_data = raw_data.map(lambda l: l.split(","))
row_data = csv_data.map(lambda p: Row(
    duration=int(p[0]), 
    protocol_type=p[1],
    service=p[2],
    flag=p[3],
    src_bytes=int(p[4]),
    dst_bytes=int(p[5])
    )
)
```

Once we have our RDD of `Row` we can infer and register the schema.  


```
interactions_df = sqlContext.createDataFrame(row_data)
interactions_df.registerTempTable("interactions")
```

Now we can run SQL queries over our data frame that has been registered as a table.  


```
# Select tcp network interactions with more than 1 second duration and no transfer from destination
tcp_interactions = sqlContext.sql("""
    SELECT duration, dst_bytes FROM interactions WHERE protocol_type = 'tcp' AND duration > 1000 AND dst_bytes = 0
""")
tcp_interactions.show()
```

    duration dst_bytes
    5057     0        
    5059     0        
    5051     0        
    5056     0        
    5051     0        
    5039     0        
    5062     0        
    5041     0        
    5056     0        
    5064     0        
    5043     0        
    5061     0        
    5049     0        
    5061     0        
    5048     0        
    5047     0        
    5044     0        
    5063     0        
    5068     0        
    5062     0        


The results of SQL queries are RDDs and support all the normal RDD operations.  


```
# Output duration together with dst_bytes
tcp_interactions_out = tcp_interactions.map(lambda p: "Duration: {}, Dest. bytes: {}".format(p.duration, p.dst_bytes))
for ti_out in tcp_interactions_out.collect():
  print ti_out
```

    Duration: 5057, Dest. bytes: 0
    Duration: 5059, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5056, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5039, Dest. bytes: 0
    Duration: 5062, Dest. bytes: 0
    Duration: 5041, Dest. bytes: 0
    Duration: 5056, Dest. bytes: 0
    Duration: 5064, Dest. bytes: 0
    Duration: 5043, Dest. bytes: 0
    Duration: 5061, Dest. bytes: 0
    Duration: 5049, Dest. bytes: 0
    Duration: 5061, Dest. bytes: 0
    Duration: 5048, Dest. bytes: 0
    Duration: 5047, Dest. bytes: 0
    Duration: 5044, Dest. bytes: 0
    Duration: 5063, Dest. bytes: 0
    Duration: 5068, Dest. bytes: 0
    Duration: 5062, Dest. bytes: 0
    Duration: 5046, Dest. bytes: 0
    Duration: 5052, Dest. bytes: 0
    Duration: 5044, Dest. bytes: 0
    Duration: 5054, Dest. bytes: 0
    Duration: 5039, Dest. bytes: 0
    Duration: 5058, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5032, Dest. bytes: 0
    Duration: 5063, Dest. bytes: 0
    Duration: 5040, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5066, Dest. bytes: 0
    Duration: 5044, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5036, Dest. bytes: 0
    Duration: 5055, Dest. bytes: 0
    Duration: 2426, Dest. bytes: 0
    Duration: 5047, Dest. bytes: 0
    Duration: 5057, Dest. bytes: 0
    Duration: 5037, Dest. bytes: 0
    Duration: 5057, Dest. bytes: 0
    Duration: 5062, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5053, Dest. bytes: 0
    Duration: 5064, Dest. bytes: 0
    Duration: 5044, Dest. bytes: 0
    Duration: 5051, Dest. bytes: 0
    Duration: 5033, Dest. bytes: 0
    Duration: 5066, Dest. bytes: 0
    Duration: 5063, Dest. bytes: 0
    Duration: 5056, Dest. bytes: 0
    Duration: 5042, Dest. bytes: 0
    Duration: 5063, Dest. bytes: 0
    Duration: 5060, Dest. bytes: 0
    Duration: 5056, Dest. bytes: 0
    Duration: 5049, Dest. bytes: 0
    Duration: 5043, Dest. bytes: 0
    Duration: 5039, Dest. bytes: 0
    Duration: 5041, Dest. bytes: 0
    Duration: 42448, Dest. bytes: 0
    Duration: 42088, Dest. bytes: 0
    Duration: 41065, Dest. bytes: 0
    Duration: 40929, Dest. bytes: 0
    Duration: 40806, Dest. bytes: 0
    Duration: 40682, Dest. bytes: 0
    Duration: 40571, Dest. bytes: 0
    Duration: 40448, Dest. bytes: 0
    Duration: 40339, Dest. bytes: 0
    Duration: 40232, Dest. bytes: 0
    Duration: 40121, Dest. bytes: 0
    Duration: 36783, Dest. bytes: 0
    Duration: 36674, Dest. bytes: 0
    Duration: 36570, Dest. bytes: 0
    Duration: 36467, Dest. bytes: 0
    Duration: 36323, Dest. bytes: 0
    Duration: 36204, Dest. bytes: 0
    Duration: 32038, Dest. bytes: 0
    Duration: 31925, Dest. bytes: 0
    Duration: 31809, Dest. bytes: 0
    Duration: 31709, Dest. bytes: 0
    Duration: 31601, Dest. bytes: 0
    Duration: 31501, Dest. bytes: 0
    Duration: 31401, Dest. bytes: 0
    Duration: 31301, Dest. bytes: 0
    Duration: 31194, Dest. bytes: 0
    Duration: 31061, Dest. bytes: 0
    Duration: 30935, Dest. bytes: 0
    Duration: 30835, Dest. bytes: 0
    Duration: 30735, Dest. bytes: 0
    Duration: 30619, Dest. bytes: 0
    Duration: 30518, Dest. bytes: 0
    Duration: 30418, Dest. bytes: 0
    Duration: 30317, Dest. bytes: 0
    Duration: 30217, Dest. bytes: 0
    Duration: 30077, Dest. bytes: 0
    Duration: 25420, Dest. bytes: 0
    Duration: 22921, Dest. bytes: 0
    Duration: 22821, Dest. bytes: 0
    Duration: 22721, Dest. bytes: 0
    Duration: 22616, Dest. bytes: 0
    Duration: 22516, Dest. bytes: 0
    Duration: 22416, Dest. bytes: 0
    Duration: 22316, Dest. bytes: 0
    Duration: 22216, Dest. bytes: 0
    Duration: 21987, Dest. bytes: 0
    Duration: 21887, Dest. bytes: 0
    Duration: 21767, Dest. bytes: 0
    Duration: 21661, Dest. bytes: 0
    Duration: 21561, Dest. bytes: 0
    Duration: 21455, Dest. bytes: 0
    Duration: 21334, Dest. bytes: 0
    Duration: 21223, Dest. bytes: 0
    Duration: 21123, Dest. bytes: 0
    Duration: 20983, Dest. bytes: 0
    Duration: 14682, Dest. bytes: 0
    Duration: 14420, Dest. bytes: 0
    Duration: 14319, Dest. bytes: 0
    Duration: 14198, Dest. bytes: 0
    Duration: 14098, Dest. bytes: 0
    Duration: 13998, Dest. bytes: 0
    Duration: 13898, Dest. bytes: 0
    Duration: 13796, Dest. bytes: 0
    Duration: 13678, Dest. bytes: 0
    Duration: 13578, Dest. bytes: 0
    Duration: 13448, Dest. bytes: 0
    Duration: 13348, Dest. bytes: 0
    Duration: 13241, Dest. bytes: 0
    Duration: 13141, Dest. bytes: 0
    Duration: 13033, Dest. bytes: 0
    Duration: 12933, Dest. bytes: 0
    Duration: 12833, Dest. bytes: 0
    Duration: 12733, Dest. bytes: 0
    Duration: 12001, Dest. bytes: 0
    Duration: 5678, Dest. bytes: 0
    Duration: 5010, Dest. bytes: 0
    Duration: 1298, Dest. bytes: 0
    Duration: 1031, Dest. bytes: 0
    Duration: 36438, Dest. bytes: 0


We can easily have a look at our data frame schema using `printSchema`.  


```
interactions_df.printSchema()
```

    root
     |-- dst_bytes: long (nullable = true)
     |-- duration: long (nullable = true)
     |-- flag: string (nullable = true)
     |-- protocol_type: string (nullable = true)
     |-- service: string (nullable = true)
     |-- src_bytes: long (nullable = true)
    


## Queries as `DataFrame` operations

Spark `DataFrame` provides a domain-specific language for structured data manipulation. This language includes methods we can concatenate in order to do selection, filtering, grouping, etc. For example, let's say we want to count how many interactions are there for each protocol type. We can proceed as follows.  


```
from time import time

t0 = time()
interactions_df.select("protocol_type", "duration", "dst_bytes").groupBy("protocol_type").count().show()
tt = time() - t0

print "Query performed in {} seconds".format(round(tt,3))
```

    protocol_type count 
    udp           20354 
    tcp           190065
    icmp          283602
    Query performed in 20.568 seconds


Now imagine that we want to count how many interactions last more than 1 second, with no data transfer from destination, grouped by protocol type. We can just add to filter calls to the previous.   


```
t0 = time()
interactions_df.select("protocol_type", "duration", "dst_bytes").filter(interactions_df.duration>1000).filter(interactions_df.dst_bytes==0).groupBy("protocol_type").count().show()
tt = time() - t0

print "Query performed in {} seconds".format(round(tt,3))
```

    protocol_type count
    tcp           139  
    Query performed in 16.641 seconds


We can use this to perform some [exploratory data analysis](http://en.wikipedia.org/wiki/Exploratory_data_analysis). Let's count how many attack and normal interactions we have. First we need to add the label column to our data.    


```
def get_label_type(label):
    if label!="normal.":
        return "attack"
    else:
        return "normal"
    
row_labeled_data = csv_data.map(lambda p: Row(
    duration=int(p[0]), 
    protocol_type=p[1],
    service=p[2],
    flag=p[3],
    src_bytes=int(p[4]),
    dst_bytes=int(p[5]),
    label=get_label_type(p[41])
    )
)
interactions_labeled_df = sqlContext.createDataFrame(row_labeled_data)
```

This time we don't need to register the schema since we are going to use the OO query interface.  

Let's check the previous actually works by counting attack and normal data in our data frame.  


```
t0 = time()
interactions_labeled_df.select("label").groupBy("label").count().show()
tt = time() - t0

print "Query performed in {} seconds".format(round(tt,3))
```

    label  count 
    attack 396743
    normal 97278 
    Query performed in 17.325 seconds


Now we want to count them by label and protocol type, in order to see how important the protocol type is to detect when an interaction is or not an attack.  


```
t0 = time()
interactions_labeled_df.select("label", "protocol_type").groupBy("label", "protocol_type").count().show()
tt = time() - t0

print "Query performed in {} seconds".format(round(tt,3))
```

    label  protocol_type count 
    attack udp           1177  
    attack tcp           113252
    attack icmp          282314
    normal udp           19177 
    normal tcp           76813 
    normal icmp          1288  
    Query performed in 17.253 seconds


At first sight it seems that *udp* interactions are in lower proportion between network attacks versus other protocol types.  

And we can do much more sophisticated groupings. For example, add to the previous a "split" based on data transfer from target.   


```
t0 = time()
interactions_labeled_df.select("label", "protocol_type", "dst_bytes").groupBy("label", "protocol_type", interactions_labeled_df.dst_bytes==0).count().show()
tt = time() - t0

print "Query performed in {} seconds".format(round(tt,3))
```

    label  protocol_type (dst_bytes = 0) count 
    normal icmp          true            1288  
    attack udp           true            1166  
    attack udp           false           11    
    normal udp           true            3594  
    normal udp           false           15583 
    attack tcp           true            110583
    attack tcp           false           2669  
    normal tcp           true            9313  
    normal tcp           false           67500 
    attack icmp          true            282314
    Query performed in 17.284 seconds


We see how relevant is this new split to determine if a network interaction is an attack.  

We will stop here, but we can see how powerful this type of queries are in order to explore our data. Actually we can replicate all the splits we saw in previous notebooks, when introducing classification trees, just by selecting, groping, and filtering our dataframe. For a more detailed (but less real-world) list of Spark's `DataFrame` operations and data sources, have a look at the official documentation [here](https://spark.apache.org/docs/latest/sql-programming-guide.html#dataframe-operations).    
