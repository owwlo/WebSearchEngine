rm -f prf*.tsv
i=0
while read q ; do
	i=$((i + 1));
	prfout=prf-$i.tsv;
	Q=`echo $q | sed -e "s/ /%20/g" -e "s/\"/%22/g"`
	curl "http://localhost:25807/prf?query=$Q&ranker=comprehensive&numdocs=10&numterms=5" > $prfout;
	echo $q:$prfout >> prf.tsv
	done < queries.tsv
java -cp src edu.nyu.cs.cs2580.Bhattacharyya prf.tsv qsim.tsv
