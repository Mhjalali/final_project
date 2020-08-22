rm report.gz.tar
wget https://loh.istgahesalavati.ir/report.gz.tar
tar -C /var/tmp/final_project -zxvf report.gz.tar
gcc dbsaver.c -o dbserver.out
./dbserver.out
rm /var/tmp/final_project/*
