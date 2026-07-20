// =====================================================================
// MongoDB — Création de l'utilisateur applicatif
// Tourné après 01_init_collections.js
// =====================================================================

db = db.getSiblingDB(process.env.MONGO_DB || "stripe_nosql");

const appUser = process.env.MONGO_APP_USER || "stripe_app";
const appPwd  = process.env.MONGO_APP_PASSWORD || "stripe_app_dev";

try {
    db.createUser({
        user: appUser,
        pwd: appPwd,
        roles: [{ role: "readWrite", db: process.env.MONGO_DB || "stripe_nosql" }]
    });
    print("✅ Utilisateur applicatif " + appUser + " créé");
} catch (e) {
    if (e.code === 51024) {  // UserAlreadyExists
        db.changeUserPassword(appUser, appPwd);
        print("✅ Mot de passe de " + appUser + " mis à jour");
    } else {
        throw e;
    }
}
