## Download the dataset on the user's home folder:

```
$ cd ~
$ wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
```

## Extract the zip file and rename the directory to Movies:

```
$ nice unzip -j "ml-latest.zip"
$ mv ml-latest Movies
``` 

## Configure Java loader with the movies path

```
JavaRDD<String> lines = sc.textFile(System.getProperty("user.home") + "/Movies/movies.csv")
```
