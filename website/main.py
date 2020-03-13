from flask import Flask,render_template,flash,request,url_for,redirect
import search as util

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/query", methods=["POST"])
def results():
    queryUser=request.form.get("query")

    urlss = util.flaskBackendQuery(queryUser)

    listUrlOnly = list()
    for doc in urlss:
        listUrlOnly.append(doc[0])  # tuple stucture (url, score), just want to display the url

    # urlss=["https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
    #       "https://www.w3schools.com/html/"]

    return render_template('results.html', urls=listUrlOnly, query=queryUser) # query=queryUser is used to show what user typed on page results.html


if __name__ == "__main__":
    app.run(debug=True)
