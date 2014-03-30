These are the instructions for installing oracle instantclient that worked for me.

go-oci
------------

following the steps at
https://github.com/Centny/Centny/blob/master/Articles/How%20build%20github.com:mattn:go-oci8.md

I had to make some changes

sudo mv libclntsh.so.12.1 libclntsh.so

sudo ln /usr/lib/instantclient_12_1/libclntsh.so /usr/lib/libclntsh.so
sudo ln /usr/lib/instantclient_12_1/libclntsh.so /usr/lib/libclntsh.so.12.1
sudo ln /usr/lib/instantclient_12_1/libocci.so.12.1 /usr/lib/libocci.so.12.1
sudo ln /usr/lib/instantclient_12_1/libociei.so /usr/lib/libociei.so
sudo ln /usr/lib/instantclient_12_1/libnnz12.so /usr/lib/libnnz12.so
sudo ln /usr/lib/instantclient_12_1/libons.so /usr/lib/libons.so
sudo ln /usr/lib/instantclient_12_1/libclntshcore.so.12.1 /usr/lib/libclntshcore.so.12.1

Essentialy you have to link all your libs to /usr/lib/*. 
If you notice, the second link is the same lib as the first, but it was the only way to remove all bugs.

goracle
---------------
Since the previous changes were in place I ammended the instructions from the goracle site at https://github.com/tgulacsi/goracle

1) execute the above instructions

2) add the following to ~/.profile
	export CGO_CFLAGS=-I/usr/lib/instantclient_12_1/sdk/include

A good idea would be to point the CGO_CFLAGS to a more generic place like /usr/include
Note to self: make links from all *.h in /usr/lib/instantclient_12_1/sdk/include to /usr/include
