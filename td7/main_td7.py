from mini_sgbd import MiniSGBD
import os

def test_recovery():
    """Test complet de la récupération après crash"""
    
    # Nettoyer les fichiers existants
    for file in ["etudiants.db", "transaction.log"]:
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                pass
    
    print("=" * 60)
    print("TEST 1: Transactions commitées avant crash")
    print("=" * 60)
    
    db = MiniSGBD("etudiants.db")
    
    # Transaction 1 : Insérer des étudiants et commiter
    db.begin()
    db.insert_record_sync("Etudiant 1")
    db.insert_record_sync("Etudiant 2")
    db.insert_record_sync("Etudiant 3")
    db.commit()
    print("✓ Transaction 1 commitée : 3 étudiants insérés")
    
    # Créer un checkpoint
    db.checkpoint()
    
    # Transaction 2 : Insérer d'autres étudiants et commiter
    db.begin()
    db.insert_record_sync("Etudiant 4")
    db.insert_record_sync("Etudiant 5")
    db.commit()
    print("✓ Transaction 2 commitée : 2 étudiants supplémentaires")
    
    # Transaction 3 : Commencer mais ne pas commiter (sera perdue)
    db.begin()
    db.insert_record_sync("Etudiant 6")
    db.insert_record_sync("Etudiant 7")
    print("✓ Transaction 3 en cours (non commitée)")
    
    # IMPORTANT : Écrire le journal partiellement pour simuler un crash réaliste
    # En situation réelle, les entrées BEGIN et INSERT seraient dans le FJT
    db._write_log_to_file()
    
    # Vérification avant crash
    print("\nÉtat avant crash :")
    print(f"  Record 0 : {db.read_record(0)}")
    print(f"  Record 4 : {db.read_record(4)}")
    
    # Simuler un crash
    print("\n" + "=" * 60)
    db.crash()
    print("=" * 60)
    
    # Récupération
    db2 = MiniSGBD("etudiants.db")
    db2.recover()
    
    # Vérification après récupération
    print("\n" + "=" * 60)
    print("VÉRIFICATION APRÈS RÉCUPÉRATION")
    print("=" * 60)
    print(f"Record 0 : {db2.read_record(0)}")
    print(f"Record 1 : {db2.read_record(1)}")
    print(f"Record 2 : {db2.read_record(2)}")
    print(f"Record 3 : {db2.read_record(3)}")
    print(f"Record 4 : {db2.read_record(4)}")
    
    db2.close()
    
    print("\n" + "=" * 60)
    print("TEST 2: Transactions avec UPDATE")
    print("=" * 60)
    
    # Attendre et nettoyer proprement
    import time
    time.sleep(0.5)
    
    for file in ["etudiants.db", "transaction.log"]:
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                pass
    
    db3 = MiniSGBD("etudiants.db")
    
    # Insérer des données initiales
    db3.begin()
    db3.insert_record_sync("Original 1")
    db3.insert_record_sync("Original 2")
    db3.commit()
    print("✓ Données initiales insérées")
    
    # Créer un checkpoint
    db3.checkpoint()
    
    # Mettre à jour et commiter
    db3.begin()
    db3.update_record(0, "Modifié 1")
    db3.commit()
    print("✓ Record 0 mis à jour et commité")
    
    # Mettre à jour sans commiter
    db3.begin()
    db3.update_record(1, "Modifié 2 (non commité)")
    print("✓ Record 1 mis à jour mais non commité")
    
    # Écrire le journal pour simuler un crash réaliste
    db3._write_log_to_file()
    
    # Crash
    print("\n" + "=" * 60)
    db3.crash()
    print("=" * 60)
    
    # Récupération
    db4 = MiniSGBD("etudiants.db")
    db4.recover()
    
    # Vérification
    print("\n" + "=" * 60)
    print("VÉRIFICATION APRÈS RÉCUPÉRATION")
    print("=" * 60)
    print(f"Record 0 : {db4.read_record(0)} (devrait être 'Modifié 1')")
    print(f"Record 1 : {db4.read_record(1)} (devrait être 'Original 2')")
    
    db4.close()
    
    print("\n" + "=" * 60)
    print("TEST 3: Rollback explicite")
    print("=" * 60)
    
    # Attendre et nettoyer proprement
    time.sleep(0.5)
    
    for file in ["etudiants.db", "transaction.log"]:
        if os.path.exists(file):
            try:
                os.remove(file)
            except:
                pass
    
    db5 = MiniSGBD("etudiants.db")
    
    # Transaction commitée
    db5.begin()
    db5.insert_record_sync("Etudiant A")
    db5.commit()
    print("✓ Transaction commitée : Etudiant A")
    
    # Transaction avec rollback
    db5.begin()
    db5.insert_record_sync("Etudiant B (sera annulé)")
    print("✓ Insertion de Etudiant B")
    db5.rollback()
    print("✓ Rollback effectué")
    
    # Vérification
    print(f"Record 0 : {db5.read_record(0)} (devrait être 'Etudiant A')")
    try:
        print(f"Record 1 : {db5.read_record(1)} (devrait être vide ou erreur)")
    except:
        print("Record 1 : N'existe pas (normal)")
    
    db5.close()
    
    print("\n" + "=" * 60)
    print("TOUS LES TESTS TERMINÉS")
    print("=" * 60)


if __name__ == "__main__":
    test_recovery()
