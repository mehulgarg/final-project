if [ -d "build" ]
then
	cd build
	make
else
	mkdir build
	cd build
	../configure --with-mesos=/usr/local/include/mesos
	make
	chmod +x start_master.sh
	mv start_master.sh ../
fi
