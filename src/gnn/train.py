import os
import random
import torch
import torch.nn.functional as F
from torch_geometric.loader import DataLoader
from torch_geometric.nn import GATConv

class RedditGAT(torch.nn.Module):
    # in_channels giờ đây sẽ được truyền vào linh hoạt
    def __init__(self, in_channels, hidden_channels=64, out_channels=1, heads=4):
        super(RedditGAT, self).__init__()
        self.conv1 = GATConv(in_channels, hidden_channels, heads=heads, dropout=0.2)
        self.conv2 = GATConv(hidden_channels * heads, out_channels, heads=1, concat=False, dropout=0.2)

    def forward(self, x, edge_index):
        x = F.dropout(x, p=0.2, training=self.training)
        x = F.elu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.2, training=self.training)
        x = self.conv2(x, edge_index)
        return x.view(-1) 

def train_model():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/reddit_graph_dataset.pt'))
    model_save_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/processed/gat_model_final.pth'))

    print("Đang nạp dữ liệu đồ thị...")
    checkpoint = torch.load(data_path, weights_only=False)
    dataset = checkpoint['dataset']
    y_mean = checkpoint['y_mean']
    y_std = checkpoint['y_std']
    
    # FIX #1: Chống Data Leakage & Đảm bảo Reproducibility tuyệt đối cho luồng Train
    random.seed(42)
    random.shuffle(dataset)
    
    train_size = int(0.7 * len(dataset))
    val_size = int(0.15 * len(dataset))
    
    train_dataset = dataset[:train_size]
    val_dataset = dataset[train_size:train_size + val_size]
    test_dataset = dataset[train_size + val_size:]

    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=32, shuffle=False)

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Đang huấn luyện trên thiết bị: {device}")

    # FIX #2: Tự động phát hiện số chiều đặc trưng (Dynamic in_channels)
    in_channels = dataset[0].x.shape[1]
    print(f"[INFO] Tự động cấu hình model với in_channels = {in_channels}")
    
    model = RedditGAT(in_channels=in_channels).to(device)
    
    optimizer = torch.optim.Adam(model.parameters(), lr=0.005, weight_decay=5e-4)
    criterion = torch.nn.HuberLoss(delta=1.0)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=5)

    epochs = 150
    patience = 15
    patience_counter = 0
    best_val_loss = float('inf')

    print("\nBắt đầu quá trình huấn luyện (Training Loop)...")
    
    for epoch in range(1, epochs + 1):
        model.train()
        total_train_loss = 0
        total_train_mae = 0
        total_train_nodes = 0

        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            
            out = model(batch.x, batch.edge_index)
            loss = criterion(out, batch.y)
            
            loss.backward()
            optimizer.step()
            
            num_nodes = batch.y.size(0)
            total_train_loss += loss.item() * num_nodes
            total_train_mae += F.l1_loss(out, batch.y, reduction='sum').item()
            total_train_nodes += num_nodes

        avg_train_loss = total_train_loss / total_train_nodes
        avg_train_mae = total_train_mae / total_train_nodes

        model.eval()
        total_val_loss = 0
        total_val_mae = 0
        total_val_nodes = 0
        
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                out = model(batch.x, batch.edge_index)
                loss = criterion(out, batch.y)
                
                num_nodes = batch.y.size(0)
                total_val_loss += loss.item() * num_nodes
                total_val_mae += F.l1_loss(out, batch.y, reduction='sum').item()
                total_val_nodes += num_nodes
                
        avg_val_loss = total_val_loss / total_val_nodes
        avg_val_mae = total_val_mae / total_val_nodes

        scheduler.step(avg_val_loss)

        if epoch % 5 == 0 or epoch == 1:
            print(f"Epoch {epoch:03d}/{epochs} | Train Loss (Huber): {avg_train_loss:.4f} (MAE: {avg_train_mae:.4f}) | Val Loss: {avg_val_loss:.4f} (MAE: {avg_val_mae:.4f})")

        if avg_val_loss < best_val_loss:
            best_val_loss = avg_val_loss
            patience_counter = 0
            torch.save({
                'model_state_dict': model.state_dict(),
                'y_mean': y_mean,
                'y_std': y_std,
                'in_channels': in_channels # Lưu luôn in_channels vào checkpoint để Inference khỏi đoán mò
            }, model_save_path)
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f"\n[Early Stopping] Kích hoạt tại epoch {epoch} do Validation Loss không giảm trong {patience} epoch liên tiếp.")
                break

    print("\nĐang tải lại checkpoint tốt nhất để chạy bài thi cuối (Test Set)...")
    best_checkpoint = torch.load(model_save_path, weights_only=False)
    # Khởi tạo lại model để an toàn tuyệt đối
    model = RedditGAT(in_channels=best_checkpoint['in_channels']).to(device)
    model.load_state_dict(best_checkpoint['model_state_dict'])
    model.eval()
    
    total_test_loss = 0
    total_test_mae = 0
    total_test_nodes = 0
    
    with torch.no_grad():
        for batch in test_loader:
            batch = batch.to(device)
            out = model(batch.x, batch.edge_index)
            loss = criterion(out, batch.y)
            
            num_nodes = batch.y.size(0)
            total_test_loss += loss.item() * num_nodes
            total_test_mae += F.l1_loss(out, batch.y, reduction='sum').item()
            total_test_nodes += num_nodes
            
    avg_test_loss = total_test_loss / total_test_nodes
    avg_test_mae = total_test_mae / total_test_nodes
    
    print(f"Kết quả Test Set Cuối Cùng | Huber Loss: {avg_test_loss:.4f} | MAE: {avg_test_mae:.4f}")
    print(f"Hoàn hảo! Cấu trúc đồ án đã sẵn sàng phục vụ suy luận tại: {model_save_path}")

if __name__ == "__main__":
    train_model()