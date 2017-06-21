# encoding: utf-8

import jieba
from glob import glob
import codecs


files = glob('f*')
for f in files:
	with codecs.open(f, 'r', 'utf-8') as fin:
		with codecs.open("{}.seg".format(f), 'w', 'utf-8') as fout:
			for line in fin:
				fout.write(u' '.join(jieba.cut(line.strip())) + u'\n')
