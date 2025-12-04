from mini_sgbd import MiniSGBD
import os
def test_transaction():
    db = MiniSGBD("etudiants.db")

    # Exemple 1 : ROLLBACK
    db.begin()
    db.insert_record("Etudiant 200")
    db.insert_record("Etudiant 201")
    db.rollback()  # Les pages transactionnelles sont ignorées
    print("Après rollback, Enregistrement 200 : ", db.read_record(0))  # Aucun changement sur disque

    # Exemple 2 : COMMIT
    db.begin()
    db.insert_record("Etudiant 202")
    db.insert_record("Etudiant 203")
    db.commit()  # Les pages transactionnelles sont forcées sur disque
    print("Après commit, Enregistrement 202 : ", db.read_record(0))  # Changement persistant

if __name__ == "__main__":
    if not os.path.exists("etudiants.db"):
        with open("etudiants.db", 'wb') as f:
            pass 
    test_transaction()
