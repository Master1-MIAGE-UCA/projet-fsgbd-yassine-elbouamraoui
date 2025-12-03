from mini_sgbd import MiniSGBD

def main():
    db = MiniSGBD("etudiants.db")

    # Insertion de 105 enregistrements
    for i in range(1, 106):
        db.insertRecord(f"Etudiant {i}")

    # Lecture d'un enregistrement
    print("Enregistrement 42 :", db.readRecord(41))  # 0-based

    # Lecture de pages entières
    print("Page 1 :", db.getPage(0))  # étudiants 1 à 40
    print("Page 2 :", db.getPage(1))  # étudiants 41 à 80
    print("Page 3 :", db.getPage(2))  # étudiants 81 à 105

if __name__ == "__main__":
    main()
