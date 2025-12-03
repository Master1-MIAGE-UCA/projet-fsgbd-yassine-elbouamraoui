from mini_sgbd import MiniSGBD
import os 
def main():
    db = MiniSGBD("etudiants.db")

    # Insérer un enregistrement
    db.insert_record_sync("Etudiant 1")
    db.insert_record_sync("Etudiant 2")

    # Lire un enregistrement
    print("Enregistrement 1 : ", db.read_record(0))

    # Lire une page entière
    page = db.get_page(0)
    print("Page 1 : ", page)

    db.close()  # Fermer le fichier après l'exécution

if __name__ == "__main__":
    main()
