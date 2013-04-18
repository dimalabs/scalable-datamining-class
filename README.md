scalable-datamining-class
=========================

Hi everyone, 

I will give a short introduction to the workflow required to create the patches for the assignments. Feel free to post questions and comments if something doesn't work or is unclear to you.

### Building the project

At first, you have to install Maven and git on your machine.

Next, you have to checkout the project via 

`$ git clone https://github.com/dimalabs/scalable-datamining-class.git scalable-datamining-class/`


Then, go to the project directory and build the project with maven


`$ mvn clean install`


### Running a test from commandline

For those of you that don't want to use an IDE for the homework, here is how you run a single test from the command line:

`$ mvn -Pcheck -Dtest=PrimeNumbersWritableTest clean test`

Note that you that the `check` profile must be activated via `-Pcheck`, as the tests are disabled by default.


### Git workflow for an assignment

This is how you check on which branch you are:

    $ git branch
    * master

We suggest that you create a new branch for each assignment like this:

    $ git checkout -b assignment1
    Switched to a new branch 'assignment1'


Now do your changes in this branch. Let's assume that you implemented the serialization in `PrimeNumbersWritable`. You can see which files are changed like this:


    $ git status
    # On branch assignment1
    # Changes not staged for commit:
    #   (use "git add <file>..." to update what will be committed)
    #   (use "git checkout -- <file>..." to discard changes in working directory)
    #
    #  modified:   src/main/java/de/tuberlin/dima/aim3/assignment1/PrimeNumbersWritable.java
    #
    no changes added to commit (use "git add" and/or "git commit -a")

Once you are done with your changes, you can add your file to the changes you want to have in your homework patch:

    $ git add src/main/java/de/tuberlin/dima/aim3/assignment1/PrimeNumbersWritable.java

Once you are done with your changes, you can add your file to the changes you want to have in your homework patch:

    $ git commit -m "excercise2"
    [assignment1 b0fa2c5] excercise2
     1 file changed, 1 insertion(+)

After you are done with all changes and committed everything, you need to create a patch that contains all your changes like this:

    $ git format-patch master 
    0001-excercise2.patch

The resulting file is that one that you need to upload to ISIS.

