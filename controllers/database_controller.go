/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"db-operator/controllers/postgres"
	"db-operator/controllers/user"
	"errors"
	"fmt"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clarizencloudv1beta1 "db-operator/api/v1beta1"
)

// DatabaseReconciler reconciles a Database object
type DatabaseReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

type DB interface {
	CreateDatabase() error
	DropDatabase() error
	CreateUser(user *user.User) error
	DropUser(user string) error
	Grant(user string) error
	Revoke(user string) error
	Exists() (bool, error)
	RoleUsers() ([]string, error)
	GetHost() string
}

var db DB

// +kubebuilder:rbac:groups=clarizen.cloud.clarizen.cloud,resources=databases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clarizen.cloud.clarizen.cloud,resources=databases/status,verbs=get;update;patch
// TODO: add secret permissions

func (r *DatabaseReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.Log.WithValues("database", req.NamespacedName)

	// your logic here
	var database clarizencloudv1beta1.Database
	err := r.Get(ctx, req.NamespacedName, &database)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if database.Status.Phase == "Terminating" {
		return ctrl.Result{}, nil
	}

	switch database.Spec.Type {
	case "postgres":
		db, err = postgres.NewDB(&database)
		if err != nil {
			return ctrl.Result{}, err
		}
	case "*":
		return ctrl.Result{}, errors.New("unsupported database type")
	}

	myFinalizerName := "db.clarizen.cloud"

	if database.ObjectMeta.DeletionTimestamp.IsZero() {
		if !contains(database.ObjectMeta.Finalizers, myFinalizerName) {
			database.ObjectMeta.Finalizers = append(database.ObjectMeta.Finalizers, myFinalizerName)
			if err := r.Update(context.Background(), &database); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		if contains(database.ObjectMeta.Finalizers, myFinalizerName) {
			if err := r.finalize(database); err != nil {
				return ctrl.Result{}, err
			}
			database.ObjectMeta.Finalizers = removeString(database.ObjectMeta.Finalizers, myFinalizerName)
			database.Status.Phase = "Terminating"
			if err := r.Update(context.Background(), &database); err != nil {
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, nil
		}
	}

	if database.Status.Phase != "Created" {

		exists, err := db.Exists()
		if err != nil {
			return ctrl.Result{}, err
		}
		if exists {
			r.Log.Info("Database already exists", "Database", database.Name)
		} else {
			err := db.CreateDatabase()
			if err != nil {
				return ctrl.Result{}, err
			}
			log.Info("Database was successfully created", "Database", database.Name)

			dbUser, err := user.NewUser()
			if err != nil {
				return ctrl.Result{}, err
			}
			dbUser.Username = database.Name

			err = db.CreateUser(dbUser)
			if err != nil {
				log.Error(err, "Unable to create user")
				return ctrl.Result{}, err
			}
			log.Info("User was successfully created", "User", dbUser.Username)
			//TODO: make it optional
			secret := &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", database.Name, database.Spec.Type),
					Namespace: database.Namespace,
				},
				Data: map[string][]byte{
					"database-host":     []byte(db.GetHost()),
					"database-port":     []byte("5432"),
					"database-name":     []byte(database.Name),
					"database-user":     []byte(dbUser.Username),
					"database-password": []byte(dbUser.Password),
				},
			}

			err = r.Create(ctx, secret)
			if err != nil {
				log.Error(err, "Secret was not created")
				return ctrl.Result{}, nil
			}

			database.Status.Phase = "Created"
			err = r.Update(ctx, &database)
			if err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	currentUsers, err := db.RoleUsers()
	if err != nil {
		return ctrl.Result{}, err
	}
	users := []string{database.Name}
	users = append(users, database.Spec.Users...)

	// Revoke access if users were removed from the object
	for _, u := range currentUsers {
		if !contains(users, u) {
			err = db.Revoke(u)
			if err != nil {
				log.Error(err, "Unable to revoke permissions")
				// TODO will be real error
				err = nil
			} else {
				log.Info("Revoke permissions", "User", u)
			}
		}
	}
	// Grant access for newly created users
	for _, u := range users {
		if !contains(currentUsers, u) {
			err = db.Grant(u)
			if err != nil {
				log.Error(err, "Unable to grant permissions")
				// TODO will be real error
				err = nil
			} else {
				log.Info("Grant permissions", "User", u)
			}
		}
	}

	return ctrl.Result{}, err
}

func (r *DatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clarizencloudv1beta1.Database{}).
		Complete(r)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func removeString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

func (r *DatabaseReconciler) finalize(database clarizencloudv1beta1.Database) error {
	log := r.Log.WithName("Finalizer")
	ctx := context.Background()
	secret := v1.Secret{}
	secretName := types.NamespacedName{
		Namespace: database.Namespace,
		Name:      fmt.Sprintf("%s-%s", database.Name, database.Spec.Type),
	}
	_ = r.Get(ctx, secretName, &secret)

	if err := r.Delete(ctx, &secret); err != nil {
		log.Error(err, "Secret was not deleted!")
	}
	if err := db.DropUser(database.Name); err != nil {
		log.Error(err, "User was not deleted")
	}

	if database.Spec.Drop {
		if err := db.DropDatabase(); err != nil {
			log.Error(err, "Database was not deleted")
		}
	} else {
		log.Info("Drop was set to false, database was not deleted", "Database", database.Name)
	}
	return nil
}
