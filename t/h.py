from flask import Flask,render_template,flash,request,url_for,redirect
app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/query", methods=["POST"])
def results():
    queryUser=request.form.get("query")

    urlss=["https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
          "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
          "https://www.w3schools.com/html/","https://www.w3schools.com/html/","https://www.w3schools.com/html/",
          "https://www.w3schools.com/html/"]
    return render_template('results.html',urls=urlss, query=queryUser)


if __name__ == "__main__":
    app.run(debug=True)
