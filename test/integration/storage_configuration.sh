#!/bin/bash
if [[ ! -d /hdd1_data ]]; then
  mkdir -pv /hdd1_data
fi

if [[ ! -d /hdd2_data ]]; then
  mkdir -pv /hdd2_data
fi
chown -v clickhouse:clickhouse /hdd1_data /hdd2_data
cat <<EOT > /etc/clickhouse-server/config.d/storage_configuration.xml
<yandex>
  <storage_configuration>
    <disks>
      <hdd1>
         <path>/hdd1_data/</path>
      </hdd1>
      <hdd2>
         <path>/hdd2_data/</path>
      </hdd2>
    </disks>
    <policies>
      <jbod>
        <volumes>
          <jbod_hdd_volume>
            <disk>hdd1</disk>
            <disk>hdd2</disk>
          </jbod_hdd_volume>
        </volumes>
      </jbod>
      <!-- <hot_and_cold>
        <volumes>
          <hot_volume>
            <disk>default</disk>
          </hot_volume>
          <cold_volume>
              <disk>hdd1</disk>
              <disk>hdd2</disk>
          </cold_volume>
        </volumes>
      </hot_and_cold> -->
    </policies>
  </storage_configuration>
</yandex>
EOT
