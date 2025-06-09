from flask import Flask,request,jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_
from datetime import datetime
from sqlalchemy import func
app=Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:Root%40123@localhost/coffee'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
db=SQLAlchemy(app)

class Listing(db.Model):
    __tablename__='sales'
    transaction_date=db.Column(db.Date)
    transaction_id=db.Column(db.VARCHAR,primary_key=True)
    item=db.Column(db.String(100))
    quantity=db.Column(db.Integer)
    price_per_unit=db.Column(db.Float)
    total_spent=db.Column(db.Float)
    payment_method=db.Column(db.String)       
    location=db.Column(db.String)

@app.route('/',methods=['GET'])
def get_user():
    print("get user...")
    lists=Listing.query.all()
    result=[]
    for data in lists:
        result.append({'transaction_date':data.transaction_date,
                       'transaction_id':data.transaction_id,
                       'item':data.item,
                       'quantity':data.quantity,
                       'price_per_unit':data.price_per_unit,
                       'total_spent':data.total_spent,
                       'payment_method':data.payment_method,
                       'location':data.location })
    return jsonify({'Data':result})

@app.route('/filter',methods=['GET'])
def filter_by_pay():
    pay_method=request.args.get('payment_method')
    filtered=Listing.query.filter_by(payment_method=pay_method).all()
    result=[]
    for data in filtered:  
        result.append({'transaction_date':data.transaction_date,
                       'transaction_id':data.transaction_id,
                       'item':data.item,
                       'quantity':data.quantity,
                       'price_per_unit':data.price_per_unit,
                       'total_spent':data.total_spent,
                       'payment_method':data.payment_method,
                       'location':data.location})
    return jsonify({"Filtered_Data":result})

@app.route('/location',methods=['GET'])
def filter_by_loc():
    location=request.args.get('location')
    filtered=Listing.query.filter_by(location=location).all()
    result=[]
    for data in filtered:
        result.append({'transaction_date':data.transaction_date,
                       'transaction_id':data.transaction_id,
                       'item':data.item,
                       'quantity':data.quantity,
                       'price_per_unit':data.price_per_unit,
                       'total_spent':data.total_spent,
                       'payment_method':data.payment_method,
                       'location':data.location})
    return jsonify({"Filtered_Data":result})

#getting the sum of quantity between date range
@app.route('/date',methods=['GET'])
def get_item_by_date():
    start_date=request.args.get("start")
    end_date=request.args.get("end")
    item=request.args.get("item")
    if not start_date or not end_date:
        return jsonify("Error!!!")
    try:
        start=datetime.strptime(start_date,'%Y-%m-%d')if start_date else None
        end=datetime.strptime(end_date,'%Y-%m-%d')if end_date else None
    except ValueError:
        return jsonify("Date does not exist")
    

    quantity_sum=db.session.query(func.sum(Listing.quantity)).filter(Listing.transaction_date.between(start,end),func.lower(Listing.item)==item.lower()).scalar()
    return jsonify({"start_date":start_date,
                    "end_date":end_date,
                    "item":item,
                    "Total_quantity":quantity_sum or 0})

#getting max item sold in a date range

@app.route('/max',methods=['GET'])
def get_max_item_by_date():
    start_date=request.args.get("start")
    end_date=request.args.get("end")
    item=request.args.get("item")
    if not start_date or not end_date:
        return jsonify("Error!!!")
    try:
        start=datetime.strptime(start_date,'%Y-%m-%d')if start_date else None
        end=datetime.strptime(end_date,'%Y-%m-%d')if end_date else None
    except ValueError:
        return jsonify("Value Error!!!")
    max_quantity=db.session.query(Listing.item,func.sum(Listing.quantity).label('quantity')).filter(Listing.transaction_date.between(start,end)).group_by(Listing.item).order_by(func.sum(Listing.quantity).desc()).first()

    return jsonify({"start_date":start_date,
                    "end_date":end_date,
                    "quantity":max_quantity.quantity,
                    "max_item_sold":max_quantity.item})








 
 
   
   

   

if __name__=='__main__':
    app.run(debug=True)