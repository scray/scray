import ipywidgets as widgets
#from IPython.display import display
from IPython.display import Javascript, display

import invoiceTool
import uuid


#def createInvoice(word1,word2):
#    print("test")
#    invoiceTool.createInvoice(id=word1,invoiceNumber=word2,sell="True")

class invoice_widgets():
    layout = widgets.Layout(width='auto', height='40px') #set width and height
    style = {'description_width': '150px'}
    layout = {'width': '400px'}

    def helper(a,b,c):
            print (a)

    def setvalues1(netto='', vat='') :
        print(netto)
        self.netto.value = netto
        self.vat.value = vat
            
    def createInvoice(self,word1,word2):
        #invoiceTool.createInvoice(id=word1,invoiceNumber=word2,sell="True")
        res=invoiceTool.createInvoice(id=word1,invoiceNumber=word2,received="false",sell="false")
        print(word2 + "=" + str(res))
        return "ok"
            
    #def __init__(self, param1 = str(uuid.uuid4()), param2 = ''):
    def __init__(self, param1 = '', param2 = '', peers = None):
        self.peers = peers
        #self.TextField1= widgets.Text(value='', description='Client List Name:',disabled=False, style=self.style, layout=self.layout)
        self.ledger = widgets.Dropdown(description = 'Ledger',options=['mychannel'], style=self.style, layout=self.layout)
        self.peer = widgets.Dropdown(description = 'Peer',options=[peers[0].addr,peers[1].addr], style=self.style, layout=self.layout)
        self.p1_text = widgets.Text(description = 'id',value = param1,disabled=False, style=self.style, layout=self.layout)
        # Rechnugsnnummer
        self.p2_text = widgets.Text(description = 'Rechnungsnummer',value = param2, style=self.style, layout=self.layout)
        # Country_Invoice Erstellung
        self.country_origin = widgets.Text(description = 'country (origin)',value = 'DE', style=self.style, layout=self.layout)
        self.country_receiver = widgets.Text(description = 'country (receiver)',value = '', style=self.style, layout=self.layout)
        
        # Invoice erhalten
        self.received = widgets.Checkbox(value=False, description='received (invoice)', style=self.style, layout=self.layout)

        self.order_received = widgets.Checkbox(value=False, description='received (order)', style=self.style, layout=self.layout)
        
        # Forderungsabtritt boolean (ID)
        self.forderungsabtritt = widgets.Checkbox(value=False, description='Forderungsabtritt', style=self.style, layout=self.layout)
       
        self.paid = widgets.Checkbox(value=False, description='Forderung bezahlt', style=self.style, layout=self.layout)
        # Geld erhalten auch von j anders / Forderung erhalten  (ID)
        self.forderungErhaltenVon = widgets.Text(description = 'Forderung erhalten von',value = '', style=self.style, layout=self.layout)
        self.steuerbefreiungsgrund = widgets.Dropdown(description = 'Steuerbefreieungsgrund',options=['','Ausfuhr'], style=self.style, layout=self.layout)
 
        #Umsatzsteuer abgeführt (Boolean) (wird von Finanzbehörde gesetzt)
        self.tax_received = widgets.Checkbox(value=False, description='Umsatzsteuer abgeführt', style=self.style, layout=self.layout)    
    
        # VAT- amount
        self.vat = widgets.Text(description = 'VAT',value = '', style=self.style, layout=self.layout)
        self.netto = widgets.Text(description = 'Netto',value = '', style=self.style, layout=self.layout)
        self.hash_text = widgets.Text(description = 'Hash (invoice)',value = '', style=self.style, disabled=True, layout=self.layout)
        
        self.invoicedata_actions = {self.p2_text, self.vat, self.netto, self.country_origin,self.country_receiver}
        for action in self.invoicedata_actions:
            action.disabled = True
        
        self.seller_actions = {self.forderungErhaltenVon, self.forderungsabtritt}
        
        self.notuser_actions = {self.hash_text,self.steuerbefreiungsgrund,
                                self.tax_received, 
                                self.forderungsabtritt,
                               self.forderungErhaltenVon }
        for action in self.notuser_actions:
            action.disabled = True
        
        self.blocked = False
        self.user_actions = {self.received,self.order_received, self.paid}
        for action in self.user_actions:
            action.disabled = True
        
        #self.received.on_submit(self.handle_submit) 
        self.received.observe(self.changed)
        self.order_received.observe(self.changed)
        self.peer.observe(self.changedpeer)
        
        self.p1_text.on_submit(self.handle_submit)
        self.p2_text.on_submit(self.handle_submit)
        display(self.ledger, self.peer,
                self.hash_text, self.p1_text, self.p2_text, self.vat, self.netto, self.country_origin,self.country_receiver, self.received,self.order_received,  
                self.forderungsabtritt, self.paid, self.forderungErhaltenVon,
                self.steuerbefreiungsgrund,self.tax_received)
        
    def changedpeer(self, change):   
        if change['type'] == 'change' and change['name'] == 'value':
            #print('peer')
            if self.peer.value == self.peers[0].addr:
                for action in self.seller_actions:
                    action.disabled = True
                for action in self.user_actions:
                    #print(str(action))
                    if action.value == False:
                        action.disabled = False
                    else:
                        action.disabled = True
                        #print(action)
            else:
                for action in self.user_actions:
                    action.disabled = True
                self.forderungErhaltenVon.disabled = False    
                for action in self.seller_actions:
                    #print(str(action))
                    if action.value == False:
                        action.disabled = False
                    else:
                        action.disabled = True   
        
    def changed(self, change):    
        #print(b['description'])
        #print(b.description)
        if self.blocked == True:
            print('blocked')
            return
        
        if change['type'] == 'change' and change['name'] == 'value':
            print ("changed to %s" % change['new'])
            print(change['owner'].description)
            #if change['owner'] == self.received:
            if change['owner'] in self.user_actions:    
                print('received')
                change['owner'].disabled = True
                if change['owner'] == self.received:
                    invoiceTool.markAsReceived(self.p1_text.value)
                elif change['owner'] == self.order_received:
                    invoiceTool.markOrderReceived(self.p1_text.value)
                    
        
    def handle_submit(self, text):
        #print ("Submitting:")
        #print ("Text " + str(text.value))
        key = self.p1_text.value
        word2 = self.p2_text.value
        #print(self.p1_text.value + "=" + word2)
        
        val = invoiceTool.queryInvoice(id=key)
        print (val)
        if val is None:
            self.w = self.createInvoice(key,word2)
            self.p1_text.value = str(int(key) + 1)
            self.p2_text.value = ''
        else:
            #netto=str(val['netto'])
            #self.setvalues1(netto=netto)
            print("key=" + key)
            self.blocked = True
            
            self.netto.value = str(val['netto'])
            self.vat.value = str(val['vat'])
            self.p2_text.value = str(val['invoiceNumber'])
            self.country_origin.value = str(val['countryOrigin'])
            self.country_receiver.value = str(val['countryReceiver'])
            self.hash_text.value = str(val['hash'])
            self.forderungErhaltenVon.value = str(val['forderungErhaltenVon'])
            self.steuerbefreiungsgrund.value = str(val['steuerbefreiungsgrund'])
            
            self.received.value       = val['received']
            self.order_received.value = val['receivedOrder']
            
            #self.paid.value = str(val['forderungBezahlt'])
            #self.tax_received.value = str(val['umsatzsteuerAbgefuehrt'])
            #self.forderungsabtritt.value = str(val['sell'])
            self.blocked = False
            
            print(self.peer.value, self.peers[0].addr)
            
            if self.peer.value == self.peers[0].addr:
                for action in self.seller_actions:
                    action.disabled = True
                for action in self.user_actions:
                    #print(str(action))
                    if action.value == False:
                        action.disabled = False
                    else:
                        action.disabled = True
                        #print(action)
            else:
                for action in self.user_actions:
                    action.disabled = True
                self.forderungErhaltenVon.disabled = False    
                for action in self.seller_actions:
                    #print(str(action))
                    if action.value == False:
                        action.disabled = False
                    else:
                        action.disabled = True   
                    
            #print ('fill:' + str(val))
            #return None
            self.w='true'
       
        return self.w
