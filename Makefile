all: full

full:
	    mvn clean package

clean:
	    mvn clean

test:
	    mvn clean verify

jar:
	    mvn clean package

release:
	    mvn -B release:prepare release:clean

coverage:
	    mvn clean scoverage:report

doc:
	    mvn clean scala:doc@scala-doc

see-coverage: coverage
	    cd target/site/scoverage; python -m SimpleHTTPServer

see-doc: doc
	    cd target/site/scaladocs; python -m SimpleHTTPServer
