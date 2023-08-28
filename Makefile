hys = $(wildcard structlib/*)
pys = $(patsubst %.hy, %.py, hys)

.PHONY: build
build: $(pys)
	python setup.py -v bdist_wheel

$(pys): $(hys)
	hy2py -o structlib structlib

test.py: test.hy
	hy2py -o $@ $<
