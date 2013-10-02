### Hints for assignment 1

Unfortunately, there is no working version of `MultipleInputs` in the hadoop version used in the assignment. In general, the availability of certain classes in certain Hadoop versions is a mess (a lot of stuff did not get ported to newer versions). We have to deal with that mess, by finding creative solutions. 

For the reduce-side join, a lot of ways to get it to work without MultipleInputs exist. For example, you can use a single one mapper to process both inputs at once. The input path must be built like this:

`Path input = new Path(authors.toString() + "," + books.toString());`

Inside the mapper, you can look at the currently processed input to see which part of the data you are currently processing:

    FileSplit inputSplit = (FileSplit) context.getInputSplit();
    System.out.println(inputSplit.getPath());
