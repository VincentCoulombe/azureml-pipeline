# AzureML Wrapper

## C'est quoi
Une couche d'abstraction autour des classes du AzureML sdk pour Python. Permet de simplifier l'utilisation du dit sdk en prenant soin de faire la poutine habituelle à la place de l'utilisateur.

## Comment ça marche?
La suivante est un mini-tutoriel sur comment utiliser le Wrapper.

### Intéragir avec un Workspace
La classe WorkspaceWrapper permet à l'utilisateur de :
1. Créer/se connecter à un AzureML Workspace. Des deux manières suivantes :
  - `mon_workspace = WorkspaceWrapper(ws_name, resource_group, subscription_id)`
    ```
  - config = {"ws_name":ws_name, "resource_group":resource_group, "subscription_id":subscription_id}
    mon_workspace = WorkspaceWrapper.from_config(config)
    ```
