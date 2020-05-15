#!/bin/sh

sudo apt-get update
sudo apt-get install  perl -y
perl -v
sudo apt-get install ctags -y
ctags --version
sudo apt-get install mariadb-server -y
sudo apt-get install apache2 -y
sudo apt-get install libapache2-mod-perl2 -y
sudo apt-get install libapache-db-perl -y
sudo apt-get install libapache-dbi-perl -y
sudo apt-get install glimpse -y
sudo apt-get install libfile-mmagic-perl -y
sudo a2enmod perl
sudo a2enmod dbd
sudo systemctl restart apache2
wget https://ayera.dl.sourceforge.net/project/lxr/stable/lxr-2.3.5.tgz
tar -zxf lxr-2.3.5.tgz
sudo rm lxr-2.3.5.tgz
mv lxr-2.3.5 lxr
mkdir kerneltree
cd kerneltree/
mkdir v1
cd
wget https://cdn.kernel.org/pub/linux/kernel/v5.x/linux-5.4.14.tar.xz
tar xf linux-5.4.14.tar.xz
sudo rm linux-5.4.14.tar.xz
cd linux-5.4.14/
mv * ~/kerneltree/v1/
cd
mkdir glimpse
sudo rm -rf linux-5.4.14
cd lxr/

sudo printf "\n\n\n\n\n\n\n/home/user/glimpse\n\n\n\n\n/home/user/kerneltree\n\n\n\n\nktree\n\n\n\n"| ./scripts/configure-lxr.pl --conf-out=lxr.conf lxrkernel.conf
sudo ./scripts/kernel-vars-grab.sh ~/kerneltree/
sudo printf "\n\n"| ./custom.d/initdb.sh
sudo cp custom.d/lxr.conf lxr.conf

sudo ./genxref --url=http://localhost/lxr --version=v1
sudo cp custom.d/apache-lxrserver.conf /etc/apache2/conf-available
sudo a2enconf apache-lxrserver.conf
sudo systemctl reload apache2
sudo a2dismod mpm*
sudo apt install libapache2-mpm-itk
sudo a2enmod mpm_worker
sudo systemctl restart apache2
sudo rm custom.d/initdb.sh





















