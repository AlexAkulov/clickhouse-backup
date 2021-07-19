# -*- mode: ruby -*-
# vi: set ft=ruby :
# This environment use only for reproducible tests for statefull Linux clickhouse-server installation
 
def total_cpus
  require 'etc'
  Etc.nprocessors
end

def get_provider
    provider='virtualbox'
    for arg in ARGV
        if ['hyperv','docker'].include? arg
            provider=arg
        end
    end
    return provider
end


Vagrant.configure(2) do |config|
  config.vm.box = "generic/ubuntu2004"
  config.vm.box_check_update = false

  if get_provider == "hyperv"
    config.vm.synced_folder ".", "/vagrant", type: "smb", smb_user: "#{ENV['USERNAME']}@#{ENV['USERDOMAIN']}", smb_password: ENV['PASSWORD'], mount_options: ["vers=3.0","domain=#{ENV['USERDOMAIN']}", "user=#{ENV['USERNAME']}","username=#{ENV['USERDOMAIN']}@#{ENV['USERNAME']}","password=#{ENV['PASSWORD']}"]
  else
    config.vm.synced_folder ".", "/vagrant"
  end

  if Vagrant.has_plugin?("vagrant-vbguest")
    config.vbguest.auto_update = false
  end

  if Vagrant.has_plugin?("vagrant-timezone")
    config.timezone.value = "UTC"
  end


  config.vm.provider "virtualbox" do |vb|
    vb.gui = false
    vb.cpus = total_cpus
    vb.memory = "2048"
    vb.default_nic_type = "virtio"
    vb.customize ["modifyvm", :id, "--uartmode1", "file", File::NULL ]
    vb.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
    vb.customize ["modifyvm", :id, "--natdnsproxy1", "on"]
    vb.customize ["modifyvm", :id, "--ioapic", "on"]
    vb.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 10000]
  end

  config.vm.provider "hyperv" do |hv|
    hv.cpus = total_cpus
    hv.maxmemory = "2048"
    hv.memory = "2048"
    hv.enable_virtualization_extensions = true
    hv.linked_clone = true
    hv.vm_integration_services = {
        time_synchronization: true,
    }
  end

  config.vm.define :clickhouse_backup do |clickhouse_backup|
    clickhouse_backup.vm.network "private_network", ip: "172.16.2.109", nic_type: "virtio"
    # port forwarding works only when pair with kubectl port-forward
    # clickhouse-server
    clickhouse_backup.vm.network "forwarded_port", guest_ip: "172.16.2.109", guest: 8123, host_ip: "127.0.0.1", host: 8123
    clickhouse_backup.vm.network "forwarded_port", guest_ip: "172.16.2.109", guest: 9000, host_ip: "127.0.0.1", host: 9000

    # clickhouse-backup
    clickhouse_backup.vm.network "forwarded_port", guest_ip: "172.16.2.109", guest: 7171, host_ip: "127.0.0.1", host: 7171

    clickhouse_backup.vm.host_name = "local-clickhouse-backup"
    # vagrant plugin install vagrant-disksize
    clickhouse_backup.disksize.size = '50GB'
  end

  config.vm.provision "shell", inline: <<-SHELL
    set -xeuo pipefail
    export DEBIAN_FRONTEND=noninteractive
    # make linux fast again ;)
    if [[ "0" == $(grep "mitigations" /etc/default/grub | wc -l) ]]; then
        echo 'GRUB_CMDLINE_LINUX="noibrs noibpb nopti nospectre_v2 nospectre_v1 l1tf=off nospec_store_bypass_disable no_stf_barrier mds=off tsx=on tsx_async_abort=off mitigations=off"' >> /etc/default/grub
        echo 'GRUB_CMDLINE_LINUX_DEFAULT="quiet splash noibrs noibpb nopti nospectre_v2 nospectre_v1 l1tf=off nospec_store_bypass_disable no_stf_barrier mds=off tsx=on tsx_async_abort=off mitigations=off"' >> /etc/default/grub
        grub-mkconfig
    fi
    systemctl enable systemd-timesyncd
    systemctl start systemd-timesyncd

    apt-get update
    apt-get install --no-install-recommends -y apt-transport-https ca-certificates software-properties-common curl
    apt-get install --no-install-recommends -y htop ethtool mc curl wget jq socat git make gcc libc-dev

    # clickhouse
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E0C56BD4
    add-apt-repository "deb http://repo.clickhouse.tech/deb/stable/ main/"
    apt-get install --no-install-recommends -y clickhouse-client clickhouse-server

    # golang
    export GOLANG_VERSION=1.16
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E
    add-apt-repository ppa:longsleep/golang-backports
    apt-get install --no-install-recommends -y golang-${GOLANG_VERSION}

    ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/go /bin/go
    ln -nvsf /usr/lib/go-${GOLANG_VERSION}/bin/gofmt /bin/gofmt
    mkdir -p /home/ubuntu/go/src/github.com/AlexAkulov/
    ln -nsvf /vagrant /home/ubuntu/go/src/github.com/AlexAkulov/clickhouse-backup

    # docker
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
    add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -cs) test"
    apt-get install --no-install-recommends -y docker-ce pigz

    # docker compose
    apt-get install -y --no-install-recommends python3-distutils
    curl -sL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
    python3 /tmp/get-pip.py

    pip3 install -U setuptools
    pip3 install -U docker-compose

    # gihub actions local
    curl https://raw.githubusercontent.com/nektos/act/master/install.sh | bash
    # run manual
    # act -v -r -j build -s DOCKER_REPO=<repo_name> -s DOCKER_IMAGE=clickhouse-backup -s DOCKER_USER=<docker_user> -s DOCKER_TOKEN=<docker_token> -w /vagrant

    systemctl restart clickhouse-server
    export DOCKER_IMAGE=alexakulov/clickhouse-backup
    # export DOCKER_IMAGE=altinity/clickhouse-backup:latest
    # export DOCKER_IMAGE=clickhousepro/clickhouse-backup:dev

    pwd
    cd /vagrant/
    make clean build
    mv -v clickhouse-backup/clickhouse-backup /bin/

    clickhouse-backup list local

    docker pull $DOCKER_IMAGE
    docker run -u $(id -u clickhouse) --rm --network host -v "/var/lib/clickhouse:/var/lib/clickhouse" \
       -e CLICKHOUSE_USER="default" \
       -e CLICKHOUSE_PASSWORD="" \
       -e S3_BUCKET="clickhouse-backup" \
       -e S3_ACCESS_KEY="access_key" \
       -e S3_SECRET_KEY="secret" \
       ${DOCKER_IMAGE} list local

   SHELL

end
