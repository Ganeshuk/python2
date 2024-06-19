import psycopg2
from psycopg2 import OperationalError

db = psycopg2.connect(
            user="ganesh",
            password="adrrafjqMKtNW3LwN9gCjQ",
            host="late-grivet-9239.7tc.aws-eu-central-1.cockroachlabs.cloud",
            port="26257",
            database="shoes"
        )


class Book:
    def add(self,title,author,isbn,genre,quantity):
        cur=db.cursor()
        cur.execute('''
            INSERT INTO books (title, author, isbn, genre, quantity)
            VALUES (%s, %s, %s, %s, %s)
        ''', (title, author, isbn, genre, quantity))
        db.commit()
        print("created")
    def update_book(self,isbn,title="none",author="none",genre="none",quantity="none"):
        dit={}
        if(title!="none"):
            dit["title"]=title
        if(author!="none"):
            dit["author"]=author
        if(genre!="none"):
            dit["genre"]=genre
        if(quantity!="none"):
            dit["quantity"]=quantity
        for i,j in dit.items():
            cur=db.cursor()
            cur.execute(f"update books set {i}=%s where isbn=%s",(j,isbn))
            db.commit()
        print("updated")
            
        
    
class BorrowerManager:
    

    def add_borrower(self, name, contact_details, membership_id):
        cur=db.cursor()
        cur.execute_query('''
            INSERT INTO borrowers (name, contact_details, membership_id)
            VALUES (%s, %s, %s)
        ''', (name, contact_details, membership_id))
        db.commit()
        print("Borrower added successfully")

    def update_borrower(self, membership_id, name=None, contact_details=None):
        updates = []
        params = []
        cur=db.cursor()
        if name:
            updates.append('name = %s')
            params.append(name)
        if contact_details:
            updates.append('contact_details = %s')
            params.append(contact_details)
        params.append(membership_id)
        query = f"UPDATE borrowers SET {', '.join(updates)} WHERE membership_id = %s"
        cur.execute_query(query, params)
        db.commit()
        print("Borrower updated successfully")

    def remove_borrower(self, membership_id):
        cur=db.cursor()
        cur.execute_query('DELETE FROM borrowers WHERE membership_id = %s', (membership_id,))
        db.commit()
        print("Borrower removed successfully")

class TransactionManager:
    
    
    def borrow_book(self, book_isbn, membership_id, due_date):
        cur=db.cursor()
        book = cur.fetch_all('SELECT id, quantity FROM books WHERE isbn = %s', (book_isbn,))
        if not book or book[0][1] <= 0:
            print("Book not available")
            return

        borrower = cur.fetch_all('SELECT id FROM borrowers WHERE membership_id = %s', (membership_id,))
        if not borrower:
            print("Borrower not found")
            return

        cur.execute_query('''
            INSERT INTO borrowed_books (book_id, borrower_id, due_date)
            VALUES (%s, %s, %s)
        ''', (book[0][0], borrower[0][0], due_date))
        db.commit()

        cur.execute_query('UPDATE books SET quantity = quantity - 1 WHERE id = %s', (book[0][0],))
        db.commit()
        print("Book borrowed successfully")

    def return_book(self, book_isbn, membership_id):
        cur=db.cursor()
        book = cur.fetch_all('SELECT id FROM books WHERE isbn = %s', (book_isbn,))
        if not book:
            print("Book not found")
            return

        borrower = cur.fetch_all('SELECT id FROM borrowers WHERE membership_id = %s', (membership_id,))
        if not borrower:
            print("Borrower not found")
            return

        cur.execute_query('DELETE FROM borrowed_books WHERE book_id = %s AND borrower_id = %s', (book[0][0], borrower[0][0]))
        cur.execute_query('UPDATE books SET quantity = quantity + 1 WHERE id = %s', (book[0][0],))
        print("Book returned successfully")


