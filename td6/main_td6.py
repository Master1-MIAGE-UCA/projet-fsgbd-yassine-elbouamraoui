from mini_sgbd import MiniSGBD
import os

def test_transaction():
    db = MiniSGBD("etudiants.db")

    # Test BEGIN/ROLLBACK
    db.begin()
    db.insert_record_sync("Etudiant 1")
    db.insert_record_sync("Etudiant 2")
    print("Avant rollback : ", db.read_record(0))
    db.rollback()  # Annuler les modifications
    print("Après rollback : ", db.read_record(0))  # Aucune modification sur disque

    # Test BEGIN/COMMIT
    db.begin()
    db.insert_record_sync("Etudiant 3")
    db.insert_record_sync("Etudiant 4")
    db.commit()  # Valider les modifications
    print("Après commit : ", db.read_record(0))  # Modifications présentes sur disque

if __name__ == "__main__":
    if not os.path.exists("etudiants.db"):
        with open("etudiants.db", 'wb') as f:
            pass 
    test_transaction()

