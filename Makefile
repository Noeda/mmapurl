CARGO := cargo
PREFIX := /usr/local

.PHONY: compile install deinstall

compile:
	cargo build --release

install: target/release/libmmapurl.so src/mmapurl.h
	cp target/release/libmmapurl.so $(PREFIX)/lib/libmmapurl.so
	cp src/mmapurl.h $(PREFIX)/include/mmapurl.h

deinstall:
	rm -f $(PREFIX)/lib/libmmapurl.so
	rm -f $(PREFIX)/include/mmapurl.h
