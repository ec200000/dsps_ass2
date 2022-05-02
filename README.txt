Submitted by: Tal Skopas 322593070, Itay Cohen 211896261

How to run the project:
Upload the jars of the steps to a s3 bucket.
Open a logs folder in the bucket you created.
Change the name of the bucket accordingly in the Main.java file.
Change the Ec2KeyName in the Main.java file.
Run the Main.

Steps summary:
1. Count the occurrences of all the words in 1-gram corpus
2. Count the occurrences of a word pair in 2-gram corpus.
3. Count the occurrences of a word triple in 3-gram corpus.
4. Organaize the data to be a 3-gram with its 2-grams and their occurrences.
5. Calculate the probability for a each triplet of words. 
6. Sort the data.

We also implemented a Comparator for step 6 to sort the data and a Partitioner class in steps 1-5.

Step 1:
Mapper: takes as input the 1-gram corpus and parse it line by line, and creates a line where the key is the word and the value is the word's occurrence, and creats another line with * as the key and the word's occurrence as the value. 
Reducer: unites all the occurences (the values) of the words (the keys) and counts the total number of words in the corpus by counting the values of *.

Step 2:
Mapper: takes as input the 2-gram corpus and parse it line by line, and creates a line where the key is the two words and the value is the words occurrence. 
Reducer: unites all the occurences (the values) of the words (the keys).

Step 3:
Mapper: takes as input the 3-gram corpus and parse it line by line, and creates a line where the key is the three words and the value is the words occurrence. 
Reducer: unites all the occurences (the values) of the words (the keys).

Step 4:
Mapper: takes as input the outputs of the 2nd and 3rd steps. If it's a step 2 output, we keep it as it is. If it's a step 3 output, we create two lines from each line: one with the first pair of words from the triplet and another with the second pair.
Reducer: for each pair and triplet, changes the occurences to be the occurences of the matching pair.

Step 5:
Mapper: takes as input the outputs of the 3rd and 4th steps. We just send the data to the reducer.
Reducer: the setup function brings the results of the 1st step from the hadoop machine, and then we compute the probability of each triplet according to the formula.

Step 6:
Comparator: sorts the data by the creterias ((1) by w1w2, ascending; (2) by the probability for w3, descending.)

Statistics:
Step 1 with local aggregation:
	Map input records=44400490
	Map output records=88800536
	Map output bytes=860143841
	Map output materialized bytes=7507057
	Input split bytes=1088
	Combine input records=88800536
	Combine output records=645290
	Reduce input groups=645262
	Reduce shuffle bytes=7507057
	Reduce input records=645290
	Reduce output records=645262

Step 1 without local aggregation:
	Map input records=44400490
	Map output records=88800536
	Map output bytes=860143841
	Map output materialized bytes=237822129
	Input split bytes=1088
	Reduce input groups=645262
	Reduce shuffle bytes=237822129
	Reduce input records=88800536
	Reduce output records=645262

Step 2 with local aggregation:
	Map input records=252069581
	Map output records=233334882
	Map output bytes=4707102779
	Map output materialized bytes=67053118
	Input split bytes=5304
	Combine input records=233334882
	Combine output records=4758948
	Reduce input groups=4758874
	Reduce shuffle bytes=67053118
	Reduce input records=4758948
	Reduce output records=4758874

Step 2 without local aggregation:
	Map input records=252069581
	Map output records=233334882
	Map output bytes=4707102779
	Map output materialized bytes=731772056
	Input split bytes=5304
	Reduce input groups=4758874
	Reduce shuffle bytes=731772056
	Reduce input records=233334882
	Reduce output records=4758874

Step 3 with local aggregation:
	Map input records=163471963
	Map output records=119255104
	Map output bytes=2903980410
	Map output materialized bytes=45245350
	Input split bytes=3264
	Combine input records=119255104
	Combine output records=2804000
	Reduce input groups=2803960
	Reduce shuffle bytes=45245350
	Reduce input records=2804000
	Reduce output records=2803960

Step 3 without local aggregation:
	Map input records=163471963
	Map output records=119255104
	Map output bytes=2903980410
	Map output materialized bytes=386212834
	Input split bytes=3264
	Reduce input groups=2803960
	Reduce shuffle bytes=386212834
	Reduce input records=119255104
	Reduce output records=2803960

Step 4 without local aggregation:
	Map input records=7562834
	Map output records=10366794
	Map output bytes=337187851
	Map output materialized bytes=164167937
	Input split bytes=3416
	Reduce input groups=4758875
	Reduce shuffle bytes=164167937
	Reduce input records=10366794
	Reduce output records=5163654

Step 5 without local aggregation:
	Map input records=7967614
	Map output records=7967614
	Map output bytes=296368509
	Map output materialized bytes=143677888
	Input split bytes=3416
	Reduce input groups=2803960
	Reduce shuffle bytes=143677888
	Reduce input records=7967614
	Reduce output records=2972416

Step 6 without local aggregation:
	Map input records=2972416
	Map output records=2972416
	Map output bytes=131663962
	Map output materialized bytes=81435847
	Input split bytes=875
	Reduce input groups=2972416
	Reduce shuffle bytes=81435847
	Reduce input records=2972416
	Reduce output records=2972416


Analysis:
צמד המילים - מה שכתבנו:

מה שכתבנו לעיל 0.1373269566495731	
מה שכתבנו למעלה 0.05385743086122435	
מה שכתבנו בזה 0.030989308841495276	
מה שכתבנו על 0.027439662158521474	
מה שכתבנו . 0.022043750395666414

צמד המילים - הן בתחום:
	
הן בתחום הכלכלי 0.030517268230662916	
הן בתחום המדיני 0.019839519251967384	
הן בתחום הפוליטי 0.018529484361997042	
הן בתחום החברתי 0.013022757998797254	
הן בתחום הדתי 0.011162084861585354	

צמד המילים - אין זה:

אין זה אלא 0.07148728901488681	
אין זה מקרה 0.029472386336257497	
אין זה מן 0.028329928499450745	
אין זה כי 0.024788527999550072	
אין זה נכון 0.019517374824587038

צמד המילים - בני אדם:
	
בני אדם על 0.008703518457160093	
בני אדם : 0.006185299719482735	
בני אדם ( 0.00542952501331327	
בני אדם הם 0.004409912518485718	
בני אדם שאין 0.004152477907531167	

צמד המילים - היו בו:
	
היו בו . 0.036110692047674676	
היו בו כל 0.02378024309598629	
היו בו גם 0.023605309106718694	
היו בו רק 0.016582552220230837	
היו בו שני 0.015241258181028377

צמד המילים - זמן קצר:
	
זמן קצר . 0.0567747067141758	
זמן קצר אחר 0.018942284346424552	
זמן קצר ביותר 0.017480663146584596	
זמן קצר בלבד 0.016566679708564108	
זמן קצר קודם 0.013278033273419022	

צמד המילים - כי גם:
	
כי גם הוא 0.04081798824839897	
כי גם אם 0.02792220805302787	
כי גם הם 0.019037806771118986	
כי גם את 0.017711478543344716	
כי גם זה 0.015906337068202145	

צמד המילים - מסירות נפש:

מסירות נפש של 0.03339743999658085	
מסירות נפש ממש 0.02260963113066454	
מסירות נפש למען 0.0156818937489807	
מסירות נפש זו 0.011376534794265447	
מסירות נפש וקידוש 0.009121768642250139

צמד המילים - עולה עד:

עולה עד לרקיע 0.12550616147531146	
עולה עד הכתר 0.024212115951004563	
עולה עד הדעת 0.023679241031741412	
עולה עד לב 0.02276668689371489	
עולה עד למעלה 0.021755330532951023	

צמד המילים - שאינם ראויים:

שאינם ראויים לכך 0.061479990653182694	
שאינם ראויים לאכילה 0.055095273371683774	
שאינם ראויים למאכל 0.02389946449934915	
שאינם ראויים להיות 0.016954381115695607	
שאינם ראויים לה 0.016895120552030466	

In most cases, the probabilities look very reasonable. 
In some of the  cases a '.' is one of the top 5 probabilities, which means these words come in the end of a sentence. 