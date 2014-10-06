#!/bin/bash

cd src

curl "http://localhost:25807/search?query=bing&ranker=numviews&format=text" > ../results/hw1.1-numviews.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=numviews&format=text" >> ../results/hw1.1-numviews.tsv
curl "http://localhost:25807/search?query=google&ranker=numviews&format=text" >> ../results/hw1.1-numviews.tsv
curl "http://localhost:25807/search?query=salsa&ranker=numviews&format=text" >> ../results/hw1.1-numviews.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=numviews&format=text" >> ../results/hw1.1-numviews.tsv

curl "http://localhost:25807/search?query=bing&ranker=phrase&format=text" > ../results/hw1.1-phrase.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=phrase&format=text" >> ../results/hw1.1-phrase.tsv
curl "http://localhost:25807/search?query=google&ranker=phrase&format=text" >> ../results/hw1.1-phrase.tsv
curl "http://localhost:25807/search?query=salsa&ranker=phrase&format=text" >> ../results/hw1.1-phrase.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=phrase&format=text" >> ../results/hw1.1-phrase.tsv

curl "http://localhost:25807/search?query=bing&ranker=QL&format=text" > ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=google&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=salsa&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv

curl "http://localhost:25807/search?query=bing&ranker=QL&format=text" > ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=google&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=salsa&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=QL&format=text" >> ../results/hw1.1-ql.tsv

curl "http://localhost:25807/search?query=bing&ranker=cosine&format=text" > ../results/hw1.1-vsm.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=cosine&format=text" >> ../results/hw1.1-vsm.tsv
curl "http://localhost:25807/search?query=google&ranker=cosine&format=text" >> ../results/hw1.1-vsm.tsv
curl "http://localhost:25807/search?query=salsa&ranker=cosine&format=text" >> ../results/hw1.1-vsm.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=cosine&format=text" >> ../results/hw1.1-vsm.tsv

curl "http://localhost:25807/search?query=bing&ranker=linear&format=text" > ../results/hw1.1-linear.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=linear&format=text" >> ../results/hw1.1-linear.tsv
curl "http://localhost:25807/search?query=google&ranker=linear&format=text" >> ../results/hw1.1-linear.tsv
curl "http://localhost:25807/search?query=salsa&ranker=linear&format=text" >> ../results/hw1.1-linear.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=linear&format=text" >> ../results/hw1.1-linear.tsv



curl "http://localhost:25807/search?query=bing&ranker=numviews&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-numviews.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=numviews&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-numviews.tsv
curl "http://localhost:25807/search?query=google&ranker=numviews&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-numviews.tsv
curl "http://localhost:25807/search?query=salsa&ranker=numviews&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-numviews.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=numviews&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-numviews.tsv

curl "http://localhost:25807/search?query=bing&ranker=phrase&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-phrase.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=phrase&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-phrase.tsv
curl "http://localhost:25807/search?query=google&ranker=phrase&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-phrase.tsv
curl "http://localhost:25807/search?query=salsa&ranker=phrase&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-phrase.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=phrase&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-phrase.tsv

curl "http://localhost:25807/search?query=bing&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=google&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=salsa&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv

curl "http://localhost:25807/search?query=bing&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=google&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=salsa&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=QL&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-ql.tsv

curl "http://localhost:25807/search?query=bing&ranker=cosine&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-vsm.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=cosine&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-vsm.tsv
curl "http://localhost:25807/search?query=google&ranker=cosine&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-vsm.tsv
curl "http://localhost:25807/search?query=salsa&ranker=cosine&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-vsm.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=cosine&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-vsm.tsv

curl "http://localhost:25807/search?query=bing&ranker=linear&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv > ../results/hw1.3-linear.tsv
curl "http://localhost:25807/search?query=data%20mining&ranker=linear&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-linear.tsv
curl "http://localhost:25807/search?query=google&ranker=linear&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-linear.tsv
curl "http://localhost:25807/search?query=salsa&ranker=linear&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-linear.tsv
curl "http://localhost:25807/search?query=web%20search&ranker=linear&format=text" | java edu.nyu.cs.cs2580.Evaluator /home/owwlo/Dropbox/Linux_Sync_Files/web_search_engine_git/web_search_engine_course/data/qrels.tsv >> ../results/hw1.3-linear.tsv

cd ..
