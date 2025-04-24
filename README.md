# TDE 02 - Big Data Map Reduce
## Requisitos
* Java
* Hadoop

## Configuração Hadoop Local

> nano $HADOOP_HOME/etc/hadoop/core-site.xml

então adicione:
> 
    <configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>file:///</value>
        </property>
    </configuration>

E rode o Hadoop localmente com:

> start-dfs.sh

## Rodando o projeto

Com o arquivo data.csv na pasta root rode:

> javac -classpath $(hadoop classpath) -d bin *.java

> jar -cvf seu-jar.jar -C bin/ .

> hadoop jar seu-jar.jar SuaClasseDriver

Obs: A classe java acima não deve conter a extensão .java.

Assim o Hadoop irá realizar o processamento dos dados e é possível acessar o resultado com:

> cat output/part-r-00000