import xlsxwriter

#create ecxelsheet
excel=xlsxwriter.Workbook("Contacts.xlsx")
#add sheet
wrksht=excel.add_worksheet("Contacts")
#formating cell
bg0=excel.add_format({'font_name':'Calibri',
'font_size':'11','center_across':'True','border':1,'bg_color':'red'})
bg5=bg0.set_text_wrap()
bg=excel.add_format({'bg_color':'f4c881','font_name':'Calibri',
'font_size':'11','center_across':'True','bottom':1})
bg1=excel.add_format({'bg_color':'white','font_name':'Calibri',
'font_size':'11','center_across':'True','bottom':1})
bg2=excel.add_format({'bg_color':'D7E4BC','font_name':'Calibri',
'font_size':'11','center_across':'True','bottom':1})
bg3=excel.add_format({'bg_color':'a7bfce','font_name':'Calibri',
'font_size':'11','center_across':'True','bottom':1})
#merging cell
wrksht.merge_range('A1:D1','Contact Personal Information',bg)
wrksht.merge_range('G1:L1','Work contact Information',bg2)
wrksht.merge_range('M1:R1','Personal contact Information',bg3)
#formating other cells
wrksht.write('E1',' ',bg1)
wrksht.write('F1',' ',bg1)
wrksht.write('S1',' ',bg1)
wrksht.write('T1',' ',bg1)
#creating dictionary for column headers
data={'Lastname':[],'Firstname':[],'Company':[],'Title':[],'Willing to share':[],
'Willing To Introduce':[],'Work phone':[],'Work email':[],'Work street':[],
'Work city':[],'Work state':[],'Work zip':[],'Personal street':[],
'Personal city':[],'Personal state':[],'Personal zip':[],'Mobile phone':[],'Personal email':[],'Note':[],'Note category':[]}
#iterating data over length
lis=list(data)
sz=len(lis)
for i in range(0,sz):
	j=1
	finl=wrksht.write(j,i,lis[i],bg5)
	#wrksht.write('B2',lis[i],bg0)
	



excel.close()