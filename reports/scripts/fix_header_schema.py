from foodworks.credentials import getGoogleCredentials
from foodworks.connector import GoogleSourceClient

gc = GoogleSourceClient.connect(getGoogleCredentials())

schema = {
	'PCSS' : [u'日期', u'食物回收來源', u'蔬菜', u'瓜類', u'生果', u'麵包', u'其他',
		u'其他 (公斤)', u'乾糧', u'急凍', u'調味品', u'飲品', u'其他', u'其他 (公斤)'],
	'TSWN' : [u'日期', u'食物回收來源', u'蔬菜', u'瓜類', u'豆品', u'生果', u'麵包',
		u'其他', u'其他 (公斤)',  u'乾糧', u'急凍', u'調味品', u'飲品', u'其他', u'其他 (公斤)']
	}


ss_definitions = [
	['TSWN','Collection', 2015],
	['TSWN','Collection', 2014],
	['PSC','Collection', 2015],
	['PSC','Collection', 2014]
]

for ss_def in ss_definitions:
	print 'CONNECTING >>>'
	print ss_def
	
	ss = gc.open(*ss_def)
	
	wss = ss.collect_week_sheets()

	for ws in wss:
		print 'PROCESSING >>>'
		print '> ', ws.title
		
		if ss.org == 'TSWN':
			cell_list = ws.range('A2:O2')
		elif ss.org == 'PCSS' :
			cell_list = ws.range('A2:N2')

		for i, val in enumerate(schema[ss.org]):
		    cell_list[i].value = val

		ws.update_cells(cell_list)