
require 'luasql'

db = luasql.connect{database='test'}

db:drop{
   table='people'
}

db:create{
   table='people', 
   columns={
      {'name', 'varchar(50)'}, 
      {'email', 'varchar(50)'} 
   }
}

db:insert{
   table='people',
   entries={
      {name='Paul Johnson', email='paul@js.com'},
      {name='Clement Farabet', email='clement@gmail.com'}
   }
}

entries = db:select{
   table='people'
}

print('Stored:')
printr(entries)
