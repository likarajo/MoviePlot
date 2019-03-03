# Information Retrieval

## Term Weighting
* Local: How important is the term in this document? => Term Frequency (TF)
* Global: How important is the term in the collection? => Document frequency (DF)

## TF-IDF:
* Terms that appear often in a document should get high weights : **TF**
* Terms that appear in many documents should get low weights: **IDF**

w<sub>i,j</sub> = weight assigned to term i in document j<br>
tf<sub>i,j</sub> = number of occurrence of term i in document j<br>
N = number of documents in entire collection<br>
n<sub>i</sub> = number of documents with term i<br>
