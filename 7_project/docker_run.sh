sudo docker build -t dtc_de .

sudo docker run -it --rm --name dtc_de_run -v /mnt/c/Users/Jishnu_Nair/.google/credentials:/usr/src/app/credentials dtc_de