# encoding: utf-8

"""
	reference: http://kaylinwalker.com/text-mining-south-park/
"""

import jieba
from glob import glob
import codecs
from collections import defaultdict as dd
import numpy as np

MAX_SCORE = 99999.0

def chisquare_modified(cnt_term_category, cnt_category, cnt_term, cnt_whole):
	"""
		cnt_term_category: 在 category 中 term 的出现次数
		cnt_category: 在 category 中出现的所有 terms 的总数
		cnt_term: term 在所有 categories 中出现的总数
		cnt_whole: 所有 categories 中出现的所有 terms 的总数
	"""
	E1 = cnt_term / cnt_whole * cnt_category
	E2 = cnt_term / cnt_whole * (cnt_whole - cnt_category)
	T1 = cnt_term_category * np.log(cnt_term_category / E1)
	cnt_other = cnt_term - cnt_term_category
	T2 = cnt_other * np.log(cnt_other / E2)
	LL = 2.0 * (T1 + T2)
	return LL


# counting
counts = dd(lambda : dd(float))
count_terms = dd(float)
files = glob('*.seg')
for f in files:
	with codecs.open(f, 'r', 'utf-8') as fin:
		for line in fin:
			for term in line.strip().split():
				counts[f][term] += 1
				count_terms[term] += 1

# calculate chi-square scores
cnt_whole = 0.0
cnt_categories = dd(float)
for f, terms in counts.iteritems():
	cnt_categories[f] = np.sum(np.array(terms.values()))
	cnt_whole += cnt_categories[f]

scores = dd(lambda : dd(float))
for f, terms in counts.iteritems():
	cnt_category = cnt_categories[f]
	for term, cnt in terms.iteritems():
		cnt_term = count_terms[term]
		# 如果该 term 只有本 category 中存在，那么满分 MAX_SCORE
		if cnt_term == cnt:
			# scores[f][term] = MAX_SCORE
			continue
		else:
			scores[f][term] = chisquare_modified(cnt, cnt_category, cnt_term, cnt_whole)

# result precessing
for f, terms in scores.iteritems():
	print "------------- {} -------------".format(f)
	top50 = sorted(terms.items(), key=lambda x: x[1], reverse=True)[0:50]
	for term, score in top50:
		print u"{}\t{}".format(term, score)
