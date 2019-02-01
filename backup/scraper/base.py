from __future__ import print_function
from requests import auth, get
from lxml import html
import getpass
from BeautifulTbl  import BeautifulTbl

class _base():
  def __init__(self, url, options):
    self.url = url
    self.options = options
    self.columns = []
    self.table = []

  def get(self, skip=0, args=None):
    webuser = getpass.getuser()
    webpass = getpass.getpass()
    url_options = ''
    if self.options:
       url_options = '&' + '=1&'.join(self.options) + '=1'
    page = get(self.url+url_options, auth=(webuser, webpass))
    print(page.url)
    tree = html.fromstring(page.content)
    #print(page.content)
    self.columns = list(map(str, tree.xpath('//th/text()')))[skip:]
    i_sw = self.columns.index('sw_app') if args.sw else 0
#   print(self.columns)
    len_col = len(self.columns)
    pre_vals = tree.xpath('.//pre')[skip:]
    for i in range(0, len(pre_vals)-1, len_col):
      if args.sw and pre_vals[i+i_sw].text_content() != args.sw:
        continue
#     self.table.append(list(map(lambda pre: pre.text_content(), pre_vals[i:i+len_col])))
      self.table.append(
        [pre_vals[i].text_content().split('.')[0]] + map(lambda pre: pre.text_content(), pre_vals[i+1:i+len_col])
      )
    return self.table

  def print(self):
    hline  = map(lambda x: "-"*len(x), self.columns)
    bt = BeautifulTbl(tbl=[self.columns] + [hline] + self.table, gap = 2)
    print(bt.build_tbl())

