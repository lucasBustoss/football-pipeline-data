

class Helpers():

    def treat_names(self, name): 
        if(name == 'America MG'):
            return 'America-MG'
        
        if(name == 'America Mineiro'):
            return 'America-MG'
        
        if(name == 'Atletico GO'):
            return 'Atletico-GO'
        
        if(name == 'Atletico Goianiense'):
            return 'Atletico-GO'
        
        if(name == 'Atletico Paranaense'):
            return 'Athletico-PR'
        
        if (name == 'Botafogo RJ'):
            return 'Botafogo'
        
        if (name == 'Bragantino'):
            return 'RB Bragantino'
        
        if (name == 'Flamengo RJ'):
            return 'Flamengo'
        
        if (name == 'Fortaleza EC'):
            return 'Fortaleza'
        
        if (name == 'Sao Paulo'):
            return 'São Paulo'

        if (name == 'Santos FC'):
            return 'Santos'
            
        return name

    def treat_month_names(self, month):
        if(month == 'Jan'):
            return '01'
            
        if(month == 'Fev'):
            return '02'
            
        if(month == 'Mar'):
            return '03'
            
        if(month == 'Apr'):
            return '04'
            
        if(month == 'May'):
            return '05'
            
        if(month == 'Mei'):
            return '05'
            
        if(month == 'Jun'):
            return '06'
            
        if(month == 'Jul'):
            return '07'
            
        if(month == 'Aug'):
            return '08'
            
        if(month == 'Sep'):
            return '09'
            
        if(month == 'Oct'):
            return '10'
            
        if(month == 'Nov'):
            return '11'
            
        if(month == 'Dec'):
            return '12'