
## Speed for Compressed vs Uncompressed file

Since the files from the Enamine DB are very large ~150GB each with around 650M compounds (lines) each, I tried running the code on the compressed file (15GB) to test the speed when working with bz2 compressed files. The test made very clear that the tradeoff of speed for working with smaller files is not worth it. 

I will add an option to work with compressed files in case the resources of the machine is limited and slower speeds is not unconvinient. 

```bash
Time taken for compressed file (bz2): 64.01 seconds
Time taken for uncompressed file: 3.78 seconds
```
*Output test for main.py with compressed vs uncompressed file:*



## Chunk Size test

## 10M compound dataset
![Chunk-test](../images/image.png)

Test of most optimal chunk size for 10M datapoints. For initial test with 10M datapoints we will set chunk_size = 6050

First test with chunk_size = 650 was 1 hour. 
Now 10M compounds run under 8 minutes

## 650M compound dataset

Test of most optimal chunk size for 650M datapoints
![Chunk-test](../images/Figure_1.png)

Most optimal chunksize for 650M datapoints: 110000
