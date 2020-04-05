import unittest
from app_v2 import app

class TestStringMethods(unittest.TestCase):
    
    def setUp(self):
        self.app = app.test_client() 
    
    def test_index(self):
        """inital test. ensure flask was set up correctly"""
        tester = app.test_client(self)
        response = tester.get('/', content_type='html/text')
        self.assertEqual(response.status_code, 200) 
    
    def test_prediccion_24h(self):
        tester = app.test_client(self)
        response = tester.get('/servicio/v2/prediccion/24horas/', content_type='html/text')
        self.assertEqual(response.status_code, 200) 
        self.assertTrue(len(response.json) == 24)

    def test_prediccion_48h(self):
        tester = app.test_client(self)
        response = tester.get('/servicio/v2/prediccion/48horas/', content_type='html/text')
        self.assertEqual(response.status_code, 200) 
        self.assertTrue(len(response.json) == 48)
        self.assertTrue(self.json_validator(response.json) == True)


    def test_prediccion_72h(self):
        tester = app.test_client(self)
        response = tester.get('/servicio/v2/prediccion/72horas/', content_type='html/text')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.json_validator(response.json) == True)


    def json_validator(self, data):
        success = True
        for prediccion in data:
            if 'hours' in prediccion and 'temp' in prediccion and 'hum' in prediccion:
                if prediccion['hours'] and prediccion['temp'] and prediccion['hum']:
                    success = True                
            else:
               success = False

        return success

if __name__ == '__main__':
    unittest.main()