
1. log into qradar using ssh and
 mkdir deepsight
 cd deepsight/

2. upload DeepSight-datafeeds-tools.tgz to deepsight directory and then:
 tar zxvf DeepSight-datafeeds-tools.tgz
 cd DeepSight-datafeeds-tools
 cd deepsight-feeds-1.2.0/
 python setup.py install

3. clone the repo:
 git clone https://github.com/ponquersohn/deepsight-qradar.git

4. configure
 mkdir tmp
 mkdir cert
 openssl s_client -showcerts -connect <qradarip>:443 </dev/null 2>/dev/null|openssl x509 -outform PEM > qradar.pem

5. copy and modify settings 
 cp qradar_config.json_template qradar_config.json
 vim qradar_config.json

6. start the damn thing
 python deepsight2qradar.py
