JDKPATH = /usr
LIBPATH = lib/bufmgrAssign.jar

CLASSPATH = .:..:$(LIBPATH)
BINPATH = bin
BIN = $(BINPATH)/bin
JAVAC = $(JDKPATH)/bin/javac
JAVA  = $(JDKPATH)/bin/java

PROGS = xx

all: $(PROGS)

compile: $(shell find src -name "*.java")
	$(JAVAC) -cp $(CLASSPATH) -d $(BINPATH) $(shell find src -name "*.java")

xx: compile
	$(JAVA) -cp $(CLASSPATH):$(BINPATH) tests.BMTest